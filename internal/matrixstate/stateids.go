package matrixstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// Resolver answers the federation read endpoints from the database.
type Resolver struct {
	db *store.Store
	// acls holds parsed server ACLs, keyed by the ACL *event* ID.
	//
	// Keying on the event rather than the room is what makes this safe: an
	// event's content is immutable, so a hit can never be stale, while which
	// event is current stays a live database lookup on every request. A room
	// that changes its ACL simply produces a different key.
	//
	// It is worth caching because ParseServerACL compiles a regex per allow
	// and deny entry, which measured at ~0.9ms per request — the single
	// largest cost in /state_ids, and all of it recomputing the same thing.
	acls *cache.LRU[string, *ServerACL]
}

// NewResolver builds a Resolver over the given store.
func NewResolver(db *store.Store) *Resolver {
	return &Resolver{
		db: db,
		// ACLs are small and rooms are few; this is generous.
		acls: cache.NewLRU[string, *ServerACL]("server_acls", cache.MB(8), sizeOfACL),
	}
}

// StateIDsResponse is the body of GET /_matrix/federation/v1/state_ids.
type StateIDsResponse struct {
	PDUIDs       []string `json:"pdu_ids"`
	AuthChainIDs []string `json:"auth_chain_ids"`
}

// StateIDs answers /state_ids for a remote server.
//
// origin must already be authenticated: this function trusts it and uses it for
// the access checks.
func (r *Resolver) StateIDs(ctx context.Context, origin, roomID, eventID string) (*StateIDsResponse, error) {
	if err := r.checkAccess(ctx, origin, roomID); err != nil {
		return nil, err
	}

	stateIDs, err := r.stateIDsForEvent(ctx, roomID, eventID)
	if err != nil {
		return nil, err
	}

	authChain, usedFallback, err := r.db.GetAuthChainIDsWithFallback(ctx, roomID, stateIDs)
	if err != nil {
		return nil, fmt.Errorf("auth chain: %w", err)
	}
	if usedFallback {
		authChainFallbacks.Inc()
		zerolog.Ctx(ctx).Debug().Str("room_id", roomID).
			Msg("Auth chain cover index incomplete; used recursive walk")
	}

	// Both fields are unordered sets on the wire, but always emit an array
	// rather than null: Synapse serialises an empty list, and a null would be
	// a needless difference.
	if stateIDs == nil {
		stateIDs = []string{}
	}
	if authChain == nil {
		authChain = []string{}
	}
	return &StateIDsResponse{PDUIDs: stateIDs, AuthChainIDs: authChain}, nil
}

// checkAccess enforces the two gates that guard every federation read: the
// requesting server must be in the room, and must not be barred by the room's
// server ACL.
//
// The order matters. Synapse checks membership first so that a server which is
// not in the room learns nothing about the room's ACL.
func (r *Resolver) checkAccess(ctx context.Context, origin, roomID string) error {
	partial, err := r.db.IsPartialStateRoom(ctx, roomID)
	if err != nil {
		return fmt.Errorf("partial state check: %w", err)
	}
	if partial {
		// Our membership view is incomplete, so we cannot safely decide who may
		// see what.
		return errPartialState()
	}

	inRoom, err := r.db.IsHostInRoom(ctx, roomID, origin)
	if err != nil {
		return fmt.Errorf("membership check: %w", err)
	}
	if !inRoom {
		return errHostNotInRoom()
	}

	acl, err := r.serverACL(ctx, roomID)
	if err != nil {
		return err
	}
	if !acl.Allowed(origin) {
		return errServerBanned()
	}
	return nil
}

// serverACL loads the room's m.room.server_acl, returning nil when the room has
// none, which means no restriction.
func (r *Resolver) serverACL(ctx context.Context, roomID string) (*ServerACL, error) {
	// Which event is current is always asked fresh, so a changed ACL takes
	// effect on the very next request.
	eventID, raw, err := r.db.GetCurrentStateEvent(ctx, roomID, "m.room.server_acl", "")
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load server acl: %w", err)
	}

	if acl, ok := r.acls.Get(eventID); ok {
		return acl, nil
	}
	acl, err := ParseServerACL(raw)
	if err != nil {
		// A malformed ACL event must not break the room; Synapse ignores bad
		// values rather than failing the request.
		acl = nil
	}
	// Cached either way: a malformed ACL is malformed forever, and re-parsing
	// it on every request would be the same waste as re-parsing a good one.
	r.acls.Add(eventID, acl)
	return acl, nil
}

// sizeOfACL approximates a parsed ACL's footprint. Compiled regexes dominate
// and their size is not observable, so this uses the pattern lengths as a
// stand-in — enough to keep the bound meaningful.
func sizeOfACL(acl *ServerACL) int64 {
	if acl == nil {
		return 64
	}
	n := int64(128)
	for _, re := range acl.allow {
		n += int64(len(re.String())) + 256
	}
	for _, re := range acl.deny {
		n += int64(len(re.String())) + 256
	}
	return n
}

// stateIDsForEvent returns the state *before* an event.
func (r *Resolver) stateIDsForEvent(ctx context.Context, roomID, eventID string) ([]string, error) {
	ev, err := r.db.GetEvent(ctx, eventID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, errEventNotFound(eventID)
	}
	if err != nil {
		return nil, fmt.Errorf("get event: %w", err)
	}
	if ev.RoomID != roomID {
		return nil, errEventNotInRoom(eventID, roomID)
	}
	// A rejected event failed auth, so it is not part of the room's state and
	// Synapse refuses to answer for it: get_state_ids_for_pdu loads the event
	// with allow_rejected left at its default of false, which raises a plain
	// "could not find event".
	//
	// Note /event does the opposite and serves rejected events deliberately,
	// so this check belongs here rather than in the shared event load.
	if ev.RejectedReason != "" {
		return nil, errEventNotFound(eventID)
	}
	if ev.Outlier {
		// We hold the event but not the state around it.
		return nil, errStateNotKnown(eventID)
	}

	group, err := r.db.GetStateGroupForEvent(ctx, eventID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, errStateNotKnown(eventID)
	}
	if err != nil {
		return nil, fmt.Errorf("state group: %w", err)
	}

	state, err := r.db.GetStateForGroup(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("state for group: %w", err)
	}

	// A state group includes the event itself when the event is a state event,
	// but /state_ids must return the state *before* it: the entry is replaced
	// by what it superseded, or dropped if it introduced that key.
	//
	// The adjustment is applied while building the result rather than by
	// editing the map. GetStateForGroup returns a cached, shared map, so
	// mutating it would corrupt the cache for every later request — and race
	// with concurrent readers besides.
	var skip *store.StateKey
	var replacement string
	if ev.IsStateEvent() {
		key := store.StateKey{Type: ev.Type, StateKey: *ev.StateKey}
		skip = &key
		replacement = replacesState(ev.JSON)
	}

	ids := make([]string, 0, len(state))
	for key, id := range state {
		if skip != nil && key == *skip {
			if replacement != "" {
				ids = append(ids, replacement)
			}
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// replacesState reads unsigned.replaces_state, the ID of the state event this
// one superseded. It is absent when the event introduced a new state key.
func replacesState(eventJSON []byte) string {
	var ev struct {
		Unsigned struct {
			ReplacesState string `json:"replaces_state"`
		} `json:"unsigned"`
	}
	if err := json.Unmarshal(eventJSON, &ev); err != nil {
		return ""
	}
	return ev.Unsigned.ReplacesState
}
