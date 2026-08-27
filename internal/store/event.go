package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Event is a stored event with the metadata needed to serve it.
type Event struct {
	EventID     string
	RoomID      string
	Type        string
	StateKey    *string
	RoomVersion string
	// JSON is the raw event as stored, which is what federation returns.
	JSON []byte
	// InternalMetadata is Synapse's private per-event metadata.
	InternalMetadata []byte
	// StreamOrdering positions the event in the room's stream. It is needed to
	// honour MSC4293's redact_end_ordering: a ban's redaction applies only to
	// events before the membership change that ended it.
	StreamOrdering int64
	// Outlier reports that we hold the event but not the state around it, so
	// /state and /state_ids cannot be answered at this event.
	//
	// It comes from the events table, not internal_metadata: Synapse stores it
	// as a column and copies it onto the event's metadata when loading. The
	// internal_metadata blob is empty for outliers, so reading it there would
	// silently treat every outlier as a normal event.
	Outlier       bool
	FormatVersion int
	// RejectedReason is non-empty for events that failed auth. They are still
	// stored, and /event may return them, but they are not part of room state.
	RejectedReason string
	// RedactedBy is the redaction event that redacted this one, or empty. When
	// set, the event must be served in redacted form: redaction is how a user
	// deletes a message.
	RedactedBy string
}

// IsRedacted reports whether the event has been redacted.
func (e *Event) IsRedacted() bool { return e.RedactedBy != "" }

// IsStateEvent reports whether the event carries a state key.
func (e *Event) IsStateEvent() bool { return e.StateKey != nil }

// eventQuery mirrors Synapse's event fetch, joining the JSON blob, the room's
// version and any rejection.
const eventQuery = `
	SELECT e.event_id, e.room_id, e.type, e.state_key,
	       ej.json, ej.internal_metadata, ej.format_version,
	       r.room_version, rej.reason, e.outlier, e.stream_ordering
	FROM events AS e
	  JOIN event_json AS ej USING (event_id)
	  LEFT JOIN rooms AS r ON r.room_id = e.room_id
	  LEFT JOIN rejections AS rej USING (event_id)
	WHERE e.event_id = ANY($1)`

// GetEvent loads a single event, returning ErrNotFound if we do not have it.
func (s *Store) GetEvent(ctx context.Context, eventID string) (*Event, error) {
	events, err := s.GetEvents(ctx, []string{eventID})
	if err != nil {
		return nil, err
	}
	ev, ok := events[eventID]
	if !ok {
		return nil, ErrNotFound
	}
	return ev, nil
}

// GetEvents loads events by ID, marking any that have been redacted. Events we
// do not have are simply absent from the result, matching Synapse's behaviour.
func (s *Store) GetEvents(ctx context.Context, eventIDs []string) (map[string]*Event, error) {
	events, err := s.getEventsRaw(ctx, eventIDs)
	if err != nil {
		return nil, err
	}
	if err := s.applyRedactions(ctx, events); err != nil {
		return nil, err
	}
	return events, nil
}

// getEventsRaw loads events without resolving redactions. Callers outside this
// package must use GetEvents, so that a redacted event can never be returned
// looking un-redacted.
func (s *Store) getEventsRaw(ctx context.Context, eventIDs []string) (map[string]*Event, error) {
	return s.getEventsRawOpt(ctx, eventIDs, true)
}

// getEventsRawOpt is getEventsRaw with control over whether what it reads is
// admitted to the cache.
//
// /state reads every state event in a room -- 145,000 of them here, about 97MB
// -- and admitting that would evict the entire working set that /event depends
// on, to hold events that will not be asked for again. It is the same rule as
// the filtered-state cache: a scan must not be allowed to displace a working
// set. Reading *from* the cache stays worthwhile, so only the write is
// suppressed.
func (s *Store) getEventsRawOpt(ctx context.Context, eventIDs []string, populateCache bool) (map[string]*Event, error) {
	out := make(map[string]*Event, len(eventIDs))
	if len(eventIDs) == 0 {
		return out, nil
	}

	// Serve what is cached and query only the rest. Stored event JSON does not
	// change; a later redaction is applied on top by GetEvents rather than by
	// rewriting the cached event.
	missing := eventIDs
	if s.caches.events.Enabled() {
		missing = make([]string, 0, len(eventIDs))
		for _, id := range eventIDs {
			if ev, ok := s.caches.events.Get(id); ok {
				out[id] = ev
				continue
			}
			missing = append(missing, id)
		}
		if len(missing) == 0 {
			return out, nil
		}
	}

	rows, err := s.pool.Query(ctx, eventQuery, missing)
	if err != nil {
		return nil, fmt.Errorf("store: get events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var e Event
		var formatVersion *int
		var roomVersion, rejected *string
		if err := rows.Scan(&e.EventID, &e.RoomID, &e.Type, &e.StateKey,
			&e.JSON, &e.InternalMetadata, &formatVersion, &roomVersion, &rejected,
			&e.Outlier, &e.StreamOrdering); err != nil {
			return nil, fmt.Errorf("store: scan event: %w", err)
		}
		if formatVersion != nil {
			e.FormatVersion = *formatVersion
		} else {
			// A NULL format_version means the original room version 1 format.
			e.FormatVersion = 1
		}
		if roomVersion != nil {
			e.RoomVersion = *roomVersion
		}
		if rejected != nil {
			e.RejectedReason = *rejected
		}
		out[e.EventID] = &e
		if populateCache {
			s.caches.events.Add(e.EventID, &e)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: event rows: %w", err)
	}
	return out, nil
}

// HasEvent reports whether we hold the event at all.
func (s *Store) HasEvent(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx, `SELECT 1 FROM events WHERE event_id = $1 LIMIT 1`, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: has event: %w", err)
	}
	return true, nil
}

// DefaultEventBatch bounds how many events are materialised at once.
//
// Sized so a chunk stays in the low megabytes for typical events while keeping
// the number of round trips small: the largest room here has 145,424 state
// events, which is 146 queries rather than one 97MB result set.
const DefaultEventBatch = 1000

// GetEventsBatched loads events in bounded chunks, handing each chunk to fn.
//
// This exists because /state is the one endpoint whose response cannot be held
// in memory: the largest room here resolves to about 97MB of event JSON, and
// materialising that per request is precisely what makes Synapse's /state cost
// ~53 seconds and drives its worker to half a gigabyte. Chunking keeps peak
// memory proportional to the chunk, not the room.
//
// Chunks are not admitted to the event cache -- see getEventsRawOpt. Events we
// do not have are simply absent from a chunk, matching Synapse, so fn must
// tolerate a chunk smaller than the IDs it was asked for.
//
// fn must not retain the map: it is not reused, but the events in it are the
// cached pointers, and callers have mutated shared events before (§6.1, §6.3).
func (s *Store) GetEventsBatched(ctx context.Context, eventIDs []string, batch int, fn func(map[string]*Event) error) error {
	if batch <= 0 {
		batch = DefaultEventBatch
	}
	for start := 0; start < len(eventIDs); start += batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+batch, len(eventIDs))

		events, err := s.getEventsRawOpt(ctx, eventIDs[start:end], false)
		if err != nil {
			return err
		}
		if err := s.applyRedactions(ctx, events); err != nil {
			return err
		}
		if err := fn(events); err != nil {
			return err
		}
	}
	return nil
}
