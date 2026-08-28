package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

// WithheldRedactions reports which of the given events are redaction events
// that Synapse would not serve.
//
// This is `get_events_as_list`'s redaction re-authorisation, which runs only
// when allow_rejected is false. It is therefore another asymmetry in the same
// family as rejected events: /event passes allow_rejected=True and serves
// these, while /state and /get_missing_events must withhold them.
//
// Synapse's reasoning is that a redaction may arrive before the event it
// redacts, so it is accepted up front and re-checked on read. Until the
// original turns up the redaction is assumed unauthorised, because serving it
// would let a server claim an event was deleted that it had no right to
// delete.
//
// The five rules, in Synapse's order:
//
//  1. no `redacts` at all -- a redacted redaction loses the key, and without
//     it there is nothing to authorise against
//  2. we do not hold the original
//  3. the original is an m.room.create; redactions of those are never served
//  4. the original is in a different room
//  5. the redaction is flagged for rechecking and the two senders' domains
//     differ, which the auth rules forbid
//
// Scale here: 45,374 redactions whose original is missing and 47 domain
// mismatches, out of 287,508 -- so roughly one redaction in six is withheld,
// which is why this surfaced the moment /get_missing_events walked a message
// DAG rather than a state set.
func (s *Store) WithheldRedactions(ctx context.Context, events map[string]*Event) (map[string]bool, error) {
	withheld := map[string]bool{}

	// Collect the redactions and what each claims to redact.
	targets := make(map[string]string, len(events))
	var wanted []string
	for id, ev := range events {
		if ev.Type != "m.room.redaction" {
			continue
		}
		target, ok := redactsTarget(ev.JSON)
		if !ok {
			// Rule 1.
			withheld[id] = true
			continue
		}
		targets[id] = target
		wanted = append(wanted, target)
	}
	if len(wanted) == 0 {
		return withheld, nil
	}

	// The originals are looked up unredacted and without the rejection filter:
	// this is an authorisation check on the redaction, not a decision about
	// whether the original is servable.
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, room_id, type, sender
		FROM events WHERE event_id = ANY($1)`, wanted)
	if err != nil {
		return nil, fmt.Errorf("store: redaction targets: %w", err)
	}
	defer rows.Close()

	type original struct{ room, typ, sender string }
	found := make(map[string]original, len(wanted))
	for rows.Next() {
		var id string
		var o original
		if err := rows.Scan(&id, &o.room, &o.typ, &o.sender); err != nil {
			return nil, fmt.Errorf("store: scan redaction target: %w", err)
		}
		found[id] = o
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: redaction target rows: %w", err)
	}

	for id, target := range targets {
		ev := events[id]
		o, ok := found[target]
		switch {
		case !ok:
			// Rule 2: the original has not arrived, so the redaction cannot be
			// authorised yet.
			withheld[id] = true
		case o.typ == "m.room.create":
			// Rule 3.
			withheld[id] = true
		case o.room != ev.RoomID:
			// Rule 4.
			withheld[id] = true
		case needsRedactionRecheck(ev.InternalMetadata) &&
			domainOf(o.sender) != domainOf(senderOfEvent(ev.JSON)):
			// Rule 5. The flag is cleared by Synapse when the sender is
			// allowed to redact anything under the auth rules, so a set flag
			// means the domains are the only thing authorising this.
			withheld[id] = true
		}
	}
	return withheld, nil
}

// redactsTarget reads the event a redaction claims to redact.
//
// MSC2174 moved the field from the top level into content, so both are
// checked: room version 12 events here carry content.redacts and older ones
// carry it at the top level.
func redactsTarget(body []byte) (string, bool) {
	if v := gjson.GetBytes(body, "redacts"); v.Exists() && v.Str != "" {
		return v.Str, true
	}
	if v := gjson.GetBytes(body, "content.redacts"); v.Exists() && v.Str != "" {
		return v.Str, true
	}
	return "", false
}

// needsRedactionRecheck reports Synapse's recheck_redaction flag.
//
// Synapse clears it once the redaction has been authorised, so an absent flag
// means the check has already passed rather than that it never applied.
func needsRedactionRecheck(internalMetadata []byte) bool {
	if len(internalMetadata) == 0 {
		return false
	}
	var meta struct {
		Recheck *bool `json:"recheck_redaction"`
	}
	if err := json.Unmarshal(internalMetadata, &meta); err != nil {
		return false
	}
	return meta.Recheck != nil && *meta.Recheck
}

// domainOf returns the server part of a Matrix ID, splitting on the first
// colon so a domain carrying a port survives intact.
func domainOf(id string) string {
	if i := strings.IndexByte(id, ':'); i >= 0 {
		return id[i+1:]
	}
	return ""
}
