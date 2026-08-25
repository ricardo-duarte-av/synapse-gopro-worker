package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// StateKey identifies a piece of room state.
type StateKey struct {
	Type     string
	StateKey string
}

// GetStateGroupForEvent returns the state group at an event.
func (s *Store) GetStateGroupForEvent(ctx context.Context, eventID string) (int64, error) {
	// An event's state group never changes once assigned.
	if g, ok := s.caches.eventStateGroup.Get(eventID); ok {
		return g, nil
	}

	var group *int64
	err := s.pool.QueryRow(ctx,
		`SELECT state_group FROM event_to_state_groups WHERE event_id = $1`, eventID).Scan(&group)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("store: get state group for event: %w", err)
	}
	if group == nil {
		// An outlier has a row with a NULL state group: we hold the event but
		// not the state around it.
		return 0, ErrNotFound
	}
	s.caches.eventStateGroup.Add(eventID, *group)
	return *group, nil
}

// stateGroupQuery walks the state group delta chain and takes the newest row
// per (type, state_key).
//
// State groups form a chain of deltas: a group either holds full state or a
// delta against a previous group, linked by state_group_edges. Resolving state
// means collecting the whole chain and letting the highest-numbered group win
// for each key, which DISTINCT ON with a descending sort does in one pass.
//
// Ported from Synapse's _get_state_groups_from_groups_txn.
const stateGroupQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (type, state_key) type, state_key, event_id
	FROM state_groups_state
	INNER JOIN sgs USING (state_group)
	ORDER BY type, state_key, state_group DESC`

// GetStateForGroup resolves the full state map at a state group.
//
// The returned map is shared and cached: callers MUST NOT modify it. Mutating
// it corrupts the cache for every later request and races with concurrent
// readers. Apply any per-request adjustment while reading, or copy first.
//
// state_groups_state is the largest table in a Synapse database by a wide
// margin, and the planner will choose a sequential scan over it without help.
// Synapse disables seqscan for this transaction and so must we: the query is
// otherwise pathological on a large room. SET LOCAL scopes the change to the
// transaction, which also makes it safe behind a transaction-mode pooler.
func (s *Store) GetStateForGroup(ctx context.Context, group int64) (map[StateKey]string, error) {
	// A state group's contents are fixed once written, so this needs no
	// invalidation and is the single most valuable thing to cache: resolving
	// state is by far the most expensive read we perform.
	if state, ok := s.caches.stateGroups.Get(group); ok {
		return state, nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}

	rows, err := tx.Query(ctx, stateGroupQuery, group)
	if err != nil {
		return nil, fmt.Errorf("store: state for group: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]string)
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: scan state row: %w", err)
		}
		state[k] = eventID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: state rows: %w", err)
	}
	s.caches.stateGroups.Add(group, state)
	return state, nil
}

// stateGroupFilteredQuery resolves state at a group, restricted to given event
// types.
//
// /event needs only the history visibility setting and, sometimes, the
// membership of one server. Resolving the full state map would mean reading
// every state event in the room — over a hundred thousand in the largest room
// here — for a single event lookup.
const stateGroupFilteredQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (type, state_key) type, state_key, event_id
	FROM state_groups_state
	INNER JOIN sgs USING (state_group)
	WHERE type = ANY($2)
	ORDER BY type, state_key, state_group DESC`

// GetFilteredStateForGroup resolves the state at a group for the given event
// types only.
//
// When the whole map for the group is already cached the filter is applied in
// memory, which is what Synapse does: its stateGroupCache is a dictionary cache
// and can answer a filtered lookup from a cached group. Ours held whole maps
// only, so /event's two filtered walks went to the database on every request
// even when the map was sitting in memory a few bytes away.
//
// The cache is deliberately only *read* here, never populated: resolving a
// whole state map to answer "what is the history visibility" would mean reading
// every state event in the room, which is 145k events in the largest room here.
// A miss must stay cheap.
func (s *Store) GetFilteredStateForGroup(ctx context.Context, group int64, types []string) (map[StateKey]string, error) {
	if state, ok := s.caches.stateGroups.Get(group); ok {
		storeMetrics().filteredState.WithLabelValues("types", "cache").Inc()
		return filterByTypes(state, types), nil
	}
	storeMetrics().filteredState.WithLabelValues("types", "database").Inc()
	return s.filteredState(ctx, stateGroupFilteredQuery, group, types, nil)
}

// filterByTypes selects the entries whose type is one of types.
//
// It always builds a new map. Returning the cached one — or any view onto it —
// would hand a caller something it may adjust in place, which has already been
// a correctness bug here twice.
func filterByTypes(state map[StateKey]string, types []string) map[StateKey]string {
	want := make(map[string]struct{}, len(types))
	for _, t := range types {
		want[t] = struct{}{}
	}
	out := make(map[StateKey]string)
	for k, v := range state {
		if _, ok := want[k.Type]; ok {
			out[k] = v
		}
	}
	return out
}

// filterMembersForServer selects membership entries for one server.
//
// It reproduces the SQL prefilter (LIKE '%:server') rather than checking the
// domain exactly, so both paths return the same set and the caller's exact
// domain check stays the single place that decides.
func filterMembersForServer(state map[StateKey]string, serverName string) map[StateKey]string {
	suffix := ":" + serverName
	out := make(map[StateKey]string)
	for k, v := range state {
		if k.Type == "m.room.member" && strings.HasSuffix(k.StateKey, suffix) {
			out[k] = v
		}
	}
	return out
}

// stateGroupMemberQuery is the same walk restricted to one server's membership
// events.
//
// Synapse pulls the entire membership list and filters it in Python, and notes
// in a comment that this is wasteful. Filtering by the user ID suffix in SQL
// produces the same set for far less work. The suffix match is only a
// prefilter; the exact domain is checked by the caller, since LIKE would also
// match a malformed ID such as "@a:b:server".
const stateGroupMemberQuery = `
	WITH RECURSIVE sgs(state_group) AS (
		VALUES($1::bigint)
	  UNION ALL
		SELECT prev_state_group FROM state_group_edges e, sgs s
		WHERE s.state_group = e.state_group
	)
	SELECT DISTINCT ON (type, state_key) type, state_key, event_id
	FROM state_groups_state
	INNER JOIN sgs USING (state_group)
	WHERE type = 'm.room.member' AND state_key LIKE $2
	ORDER BY type, state_key, state_group DESC`

// GetServerMembershipStateForGroup resolves membership state at a group for
// users on one server.
func (s *Store) GetServerMembershipStateForGroup(ctx context.Context, group int64, serverName string) (map[StateKey]string, error) {
	if strings.ContainsAny(serverName, "%_") {
		return nil, fmt.Errorf("store: invalid server name %q", serverName)
	}
	if state, ok := s.caches.stateGroups.Get(group); ok {
		storeMetrics().filteredState.WithLabelValues("members", "cache").Inc()
		return filterMembersForServer(state, serverName), nil
	}
	storeMetrics().filteredState.WithLabelValues("members", "database").Inc()
	return s.filteredState(ctx, stateGroupMemberQuery, group, nil, ptr("%:"+serverName))
}

// filteredState runs one of the filtered state walks.
func (s *Store) filteredState(ctx context.Context, query string, group int64, types []string, like *string) (map[StateKey]string, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("store: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Same planner hint as the unfiltered walk; without it state_groups_state
	// is scanned sequentially.
	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		return nil, fmt.Errorf("store: disable seqscan: %w", err)
	}

	var rows pgx.Rows
	if like != nil {
		rows, err = tx.Query(ctx, query, group, *like)
	} else {
		rows, err = tx.Query(ctx, query, group, types)
	}
	if err != nil {
		return nil, fmt.Errorf("store: filtered state: %w", err)
	}
	defer rows.Close()

	state := make(map[StateKey]string)
	for rows.Next() {
		var k StateKey
		var eventID string
		if err := rows.Scan(&k.Type, &k.StateKey, &eventID); err != nil {
			return nil, fmt.Errorf("store: scan state row: %w", err)
		}
		state[k] = eventID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: filtered state rows: %w", err)
	}
	return state, nil
}

func ptr[T any](v T) *T { return &v }

// AreUsersErased reports which of the given users have requested erasure.
//
// An erased user's events must not be served to other servers, redacted or
// otherwise.
func (s *Store) AreUsersErased(ctx context.Context, userIDs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT user_id FROM erased_users WHERE user_id = ANY($1)`, userIDs)
	if err != nil {
		return nil, fmt.Errorf("store: are users erased: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: scan erased user: %w", err)
		}
		out[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: erased user rows: %w", err)
	}
	return out, nil
}
