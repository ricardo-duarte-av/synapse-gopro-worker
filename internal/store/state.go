package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// StateKey identifies a piece of room state.
type StateKey struct {
	Type     string
	StateKey string
}

// GetStateGroupForEvent returns the state group at an event.
func (s *Store) GetStateGroupForEvent(ctx context.Context, eventID string) (int64, error) {
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
// state_groups_state is the largest table in a Synapse database by a wide
// margin, and the planner will choose a sequential scan over it without help.
// Synapse disables seqscan for this transaction and so must we: the query is
// otherwise pathological on a large room. SET LOCAL scopes the change to the
// transaction, which also makes it safe behind a transaction-mode pooler.
func (s *Store) GetStateForGroup(ctx context.Context, group int64) (map[StateKey]string, error) {
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
	return state, nil
}
