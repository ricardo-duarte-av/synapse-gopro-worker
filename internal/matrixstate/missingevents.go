package matrixstate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// MissingEventsResponse is the body of a /get_missing_events answer.
type MissingEventsResponse struct {
	Events []json.RawMessage `json:"events"`
}

// GetMissingEvents answers /get_missing_events for a remote server.
//
// It sits between /event and /state in almost every respect, and the
// combination is not the same as either:
//
//   - like /state and /state_ids, it checks the room's server ACL; /event does
//     not.
//   - like /event, it applies history-visibility filtering, with the same
//     flags, so an invisible event is redacted rather than withheld.
//   - like /state, it drops rejected events: it fetches through
//     get_events_as_list, whose allow_rejected defaults to false. /event is
//     the exception that serves them.
//   - like /state, it applies filter_pdus_for_valid_depth; /event does not.
//   - like /event and unlike /state, it passes a real clock to get_pdu_json,
//     so age_ts becomes age.
//
// Two things are unique to it. Partial-state rooms are *allowed*, where /event
// and the state endpoints refuse them outright -- Synapse relies on
// filter_events_for_server to remove remote events instead. And the ACL check
// runs *before* the membership check, the reverse of /state_ids, so a server
// that is not in the room can still learn that it is ACL-banned. That is
// Synapse's ordering and we mirror it rather than improving on it.
func (r *Resolver) GetMissingEvents(ctx context.Context, origin, serverName, roomID string, earliest, latest []string, limit int) (*MissingEventsResponse, error) {
	// ACL first: this is on_get_missing_events in federation_server, which
	// runs before the handler's assert_host_in_room.
	acl, err := r.serverACL(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if !acl.Allowed(origin) {
		return nil, errServerBanned()
	}

	// Partial state is permitted here, so this deliberately does not call
	// checkAccess: that rejects partial-state rooms, which would answer 403
	// where Synapse answers normally.
	inRoom, err := r.db.IsHostInRoom(ctx, roomID, origin)
	if err != nil {
		return nil, fmt.Errorf("membership check: %w", err)
	}
	if !inRoom {
		return nil, errHostNotInRoom()
	}

	partial, err := r.db.IsPartialStateRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("partial state check: %w", err)
	}

	ids, err := r.db.GetMissingEvents(ctx, roomID, earliest, latest, limit)
	if err != nil {
		return nil, fmt.Errorf("missing events: %w", err)
	}

	events, err := r.db.GetEvents(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}

	// Synapse re-authorises redactions on read when allow_rejected is false,
	// withholding any it cannot authorise. /event does not, because it passes
	// allow_rejected=True -- the same asymmetry as rejected events.
	withheld, err := r.db.WithheldRedactions(ctx, events)
	if err != nil {
		return nil, fmt.Errorf("redaction authorisation: %w", err)
	}

	now := time.Now().UnixMilli()
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		ev, ok := events[id]
		if !ok {
			// get_events_as_list silently drops what it does not hold.
			continue
		}
		if ev.RejectedReason != "" {
			// allow_rejected defaults to false.
			continue
		}
		if withheld[id] {
			continue
		}

		body, err := r.missingEventBody(ctx, ev, origin, serverName, partial, now)
		if err != nil {
			return nil, err
		}
		if !depthInCanonicalRange(body) {
			continue
		}
		out = append(out, body)
	}

	// Always an array, never null: Synapse serialises an empty list.
	return &MissingEventsResponse{Events: out}, nil
}

// missingEventBody renders one event as /get_missing_events serialises it.
func (r *Resolver) missingEventBody(ctx context.Context, ev *store.Event, origin, serverName string, partialStateRoom bool, nowMS int64) ([]byte, error) {
	visible, err := r.eventVisibleInRoom(ctx, ev, origin, serverName, partialStateRoom)
	if err != nil {
		return nil, err
	}

	body := ev.JSON
	if ev.IsRedacted() || !visible {
		body, err = redactEvent(ev)
		if err != nil {
			return nil, fmt.Errorf("redact %s: %w", ev.EventID, err)
		}
	}
	body, err = applyPDUJSONRules(body, nowMS)
	if err != nil {
		return nil, fmt.Errorf("prepare pdu %s: %w", ev.EventID, err)
	}
	return body, nil
}
