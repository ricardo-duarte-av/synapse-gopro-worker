package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// redactionRow is a redaction event targeting another event.
type redactionRow struct {
	// RedactionID is the m.room.redaction event.
	RedactionID string
	// Recheck reports that the redaction still needs authorising against the
	// redacted event, which room version 3 onwards defers until read time.
	Recheck bool
}

// getRedactions returns redaction events targeting any of the given events.
func (s *Store) getRedactions(ctx context.Context, eventIDs []string) (map[string][]redactionRow, error) {
	out := make(map[string][]redactionRow)
	if len(eventIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, redacts, recheck FROM redactions WHERE redacts = ANY($1)`, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("store: get redactions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var redactionID string
		var redacts *string
		var recheck *bool
		if err := rows.Scan(&redactionID, &redacts, &recheck); err != nil {
			return nil, fmt.Errorf("store: scan redaction: %w", err)
		}
		if redacts == nil {
			continue
		}
		out[*redacts] = append(out[*redacts], redactionRow{
			RedactionID: redactionID,
			Recheck:     recheck != nil && *recheck,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: redaction rows: %w", err)
	}
	return out, nil
}

// applyRedactions marks events that have been redacted.
//
// Ported from Synapse's _maybe_redact_event_row. Redaction is how a user
// deletes a message, so an event that has been redacted must never be served
// with its original content — which is why this lives in the store, applied to
// every event load, rather than being left to individual callers to remember.
func (s *Store) applyRedactions(ctx context.Context, events map[string]*Event) error {
	if len(events) == 0 {
		return nil
	}
	ids := make([]string, 0, len(events))
	for id := range events {
		ids = append(ids, id)
	}

	redactions, err := s.getRedactions(ctx, ids)
	if err != nil {
		return err
	}
	if len(redactions) == 0 {
		return nil
	}

	// Redaction events that still need authorising have to be loaded so their
	// sender and room can be checked.
	var needed []string
	for _, rows := range redactions {
		for _, r := range rows {
			if r.Recheck {
				needed = append(needed, r.RedactionID)
			}
		}
	}
	var redactionEvents map[string]*Event
	if len(needed) > 0 {
		// Loaded without recursing, since a redaction of a redaction is not
		// something this check needs to resolve.
		redactionEvents, err = s.getEventsRaw(ctx, needed)
		if err != nil {
			return err
		}
	}

	for id, ev := range events {
		rows, ok := redactions[id]
		if !ok {
			continue
		}
		// Synapse deliberately ignores redactions of m.room.create.
		if ev.Type == "m.room.create" {
			continue
		}
		if redactionID, ok := shouldRedact(ev, rows, redactionEvents); ok {
			ev.RedactedBy = redactionID
		}
	}
	return nil
}

// shouldRedact decides whether any of the given redactions apply.
func shouldRedact(ev *Event, rows []redactionRow, redactionEvents map[string]*Event) (string, bool) {
	// An already-authorised redaction applies without further checks.
	for _, r := range rows {
		if !r.Recheck {
			return r.RedactionID, true
		}
	}

	for _, r := range rows {
		redaction, ok := redactionEvents[r.RedactionID]
		if !ok || redaction.RejectedReason != "" {
			// We do not have the redaction, or it failed auth.
			continue
		}
		if redaction.RoomID != ev.RoomID {
			continue
		}
		// From room version 3, a redaction whose authorisation was deferred is
		// only honoured when it comes from the same server as the event it
		// redacts.
		if senderDomain(redaction.JSON) != senderDomain(ev.JSON) {
			continue
		}
		return r.RedactionID, true
	}
	return "", false
}

func senderDomain(eventJSON []byte) string {
	var ev struct {
		Sender string `json:"sender"`
	}
	if err := json.Unmarshal(eventJSON, &ev); err != nil {
		return ""
	}
	idx := strings.Index(ev.Sender, ":")
	if idx == -1 {
		return ""
	}
	return ev.Sender[idx+1:]
}
