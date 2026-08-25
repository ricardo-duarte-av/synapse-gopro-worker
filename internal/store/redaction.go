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
	// Deliberately no early return when there are no redaction rows: MSC4293
	// ban redactions are a separate source, and an event redacted by a ban has
	// no row in `redactions` at all.

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

	// MSC4293: a ban with redact_events redacts everything that user sent in
	// the room, with no per-event redaction row.
	rooms := make([]string, 0, len(events))
	users := make([]string, 0, len(events))
	senders := make(map[string]string, len(events))
	for id, ev := range events {
		sender := senderOfEvent(ev.JSON)
		if sender == "" {
			continue
		}
		senders[id] = sender
		rooms = append(rooms, ev.RoomID)
		users = append(users, sender)
	}
	bans, err := s.getBanRedactions(ctx, rooms, users)
	if err != nil {
		return err
	}

	for id, ev := range events {
		rows, ok := redactions[id]
		if !ok {
			// No redaction event, but a ban may still redact this.
			if br, banned := bans[ev.RoomID+"\x00"+senders[id]]; banned && banAppliesTo(br, ev) {
				redacted := *ev
				redacted.RedactedBy = br.RedactingEventID
				events[id] = &redacted
			}
			continue
		}
		// Synapse deliberately ignores redactions of m.room.create.
		if ev.Type == "m.room.create" {
			continue
		}
		redactionID, ok := shouldRedact(ev, rows, redactionEvents)
		if !ok {
			// A ban can still redact an event whose own redaction did not
			// pass the recheck.
			if br, banned := bans[ev.RoomID+"\x00"+senders[id]]; banned && banAppliesTo(br, ev) {
				redactionID = br.RedactingEventID
			} else {
				continue
			}
		}
		// Replace with a copy rather than mutating in place. The event may be
		// a cached pointer shared with other in-flight requests, so writing to
		// it would both race and stamp redaction state into the cache — where
		// it would then be applied to an event whose redaction had not yet been
		// checked.
		redacted := *ev
		redacted.RedactedBy = redactionID
		events[id] = &redacted
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

// banRedaction is an MSC4293 "redact events on ban" entry: every event a user
// sent in a room, up to an optional end point, is served redacted.
type banRedaction struct {
	// RedactingEventID is the ban that caused it, used the same way a
	// redaction event ID is.
	RedactingEventID string
	// EndOrdering, when non-zero, is the stream position of the membership
	// change that ended the ban's redaction. Events at or after it are not
	// redacted.
	EndOrdering int64
}

// getBanRedactions returns MSC4293 ban redactions covering the given
// (room, sender) pairs.
//
// This is a second, entirely separate source of redaction from the redactions
// table. A moderator banning a spammer with `org.matrix.msc4293.redact_events`
// redacts everything that user sent in the room at once, without a redaction
// event per message — so an event can be redacted with no row in `redactions`
// at all, its stored JSON still holding the original content.
//
// Missing this served the full content of banned users' events to remote
// servers. It was found in shadow mode against a real spam wave, and it is not
// a corner case: 9,106 rows across 122 rooms on this deployment.
func (s *Store) getBanRedactions(ctx context.Context, rooms, users []string) (map[string]banRedaction, error) {
	out := make(map[string]banRedaction)
	if len(rooms) == 0 {
		return out, nil
	}
	const q = `
		SELECT room_id, user_id, redacting_event_id, redact_end_ordering
		FROM room_ban_redactions
		WHERE (room_id, user_id) IN (SELECT * FROM unnest($1::text[], $2::text[]))`

	rows, err := s.pool.Query(ctx, q, rooms, users)
	if err != nil {
		return nil, fmt.Errorf("store: ban redactions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var roomID, userID, redactingID string
		var endOrdering *int64
		if err := rows.Scan(&roomID, &userID, &redactingID, &endOrdering); err != nil {
			return nil, fmt.Errorf("store: scan ban redaction: %w", err)
		}
		br := banRedaction{RedactingEventID: redactingID}
		if endOrdering != nil {
			br.EndOrdering = *endOrdering
		}
		out[roomID+"\x00"+userID] = br
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: ban redaction rows: %w", err)
	}
	return out, nil
}

// senderOfEvent reads the sender from stored event JSON.
func senderOfEvent(raw []byte) string {
	var ev struct {
		Sender string `json:"sender"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return ""
	}
	return ev.Sender
}

// banAppliesTo reports whether a ban redaction covers this event.
//
// A ban that was later lifted records the stream position where its redaction
// stops; events at or after it are served normally. Backfilled events have a
// negative stream ordering and so always fall before it, which is deliberate
// in Synapse and mirrored here.
func banAppliesTo(br banRedaction, ev *Event) bool {
	if ev.Type == "m.room.create" {
		// Synapse ignores redactions of m.room.create.
		return false
	}
	if br.EndOrdering != 0 && ev.StreamOrdering >= br.EndOrdering {
		return false
	}
	return true
}
