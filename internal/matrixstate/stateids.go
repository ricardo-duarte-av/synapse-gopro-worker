package matrixstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// Resolver answers the federation read endpoints from the database.
type Resolver struct {
	db *store.Store
}

// NewResolver builds a Resolver over the given store.
func NewResolver(db *store.Store) *Resolver { return &Resolver{db: db} }

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
	raw, err := r.db.GetCurrentStateEventJSON(ctx, roomID, "m.room.server_acl", "")
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load server acl: %w", err)
	}
	acl, err := ParseServerACL(raw)
	if err != nil {
		// A malformed ACL event must not break the room; Synapse ignores bad
		// values rather than failing the request.
		return nil, nil
	}
	return acl, nil
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
	// but /state_ids must return the state *before* it. Replace the event's own
	// entry with what it superseded, or drop the entry if it introduced that
	// key.
	if ev.IsStateEvent() {
		key := store.StateKey{Type: ev.Type, StateKey: *ev.StateKey}
		if prev := replacesState(ev.JSON); prev != "" {
			state[key] = prev
		} else {
			delete(state, key)
		}
	}

	ids := make([]string, 0, len(state))
	for _, id := range state {
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
