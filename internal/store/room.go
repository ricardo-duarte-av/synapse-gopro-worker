package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned when a room or event is not in the database.
var ErrNotFound = errors.New("not found")

// RoomInfo describes a room's version and indexing state.
type RoomInfo struct {
	RoomVersion string
	// HasAuthChainIndex reports whether the fast auth-chain cover index covers
	// this room. When false, the slow recursive walk over event_auth applies.
	HasAuthChainIndex bool
}

// GetRoomInfo loads a room's version and auth-chain indexing state.
func (s *Store) GetRoomInfo(ctx context.Context, roomID string) (RoomInfo, error) {
	var info RoomInfo
	var version *string
	var indexed *bool
	err := s.pool.QueryRow(ctx,
		`SELECT room_version, has_auth_chain_index FROM rooms WHERE room_id = $1`,
		roomID).Scan(&version, &indexed)
	if errors.Is(err, pgx.ErrNoRows) {
		return info, ErrNotFound
	}
	if err != nil {
		return info, fmt.Errorf("store: get room info: %w", err)
	}
	if version != nil {
		info.RoomVersion = *version
	}
	info.HasAuthChainIndex = indexed != nil && *indexed
	return info, nil
}

// IsHostInRoom reports whether the given server has a joined member in the room.
//
// This is the access check that gates /state, /state_ids and /event: a remote
// server may only read a room it is actually in. It mirrors Synapse's
// _check_host_room_membership — a LIKE on the user ID's domain suffix, followed
// by an exact check on the returned user, because LIKE alone would be a weak
// guarantee.
func (s *Store) IsHostInRoom(ctx context.Context, roomID, host string) (bool, error) {
	// A host containing LIKE wildcards could otherwise match other domains.
	if strings.ContainsAny(host, "%_") {
		return false, fmt.Errorf("store: invalid host name %q", host)
	}

	const q = `
		SELECT state_key FROM current_state_events
		WHERE membership = 'join'
		  AND type = 'm.room.member'
		  AND room_id = $1
		  AND state_key LIKE $2
		LIMIT 1`

	var userID string
	err := s.pool.QueryRow(ctx, q, roomID, "%:"+host).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: is host in room: %w", err)
	}

	// Confirm the domain exactly; the LIKE above is only a prefilter.
	if domainFromID(userID) != host {
		return false, fmt.Errorf("store: host %q did not match user %q", host, userID)
	}
	return true, nil
}

// domainFromID returns the server name from a Matrix user ID, or "" if the ID
// has no domain part.
func domainFromID(id string) string {
	_, domain, found := strings.Cut(id, ":")
	if !found {
		return ""
	}
	return domain
}

// IsPartialStateRoom reports whether we only have partial state for the room.
//
// Federation reads must be refused for such rooms: our membership view is
// incomplete, so we cannot safely decide who is allowed to see what.
func (s *Store) IsPartialStateRoom(ctx context.Context, roomID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM partial_state_rooms WHERE room_id = $1 LIMIT 1`, roomID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: is partial state room: %w", err)
	}
	return true, nil
}

// GetCurrentStateEventJSON returns the JSON of a current state event, or
// ErrNotFound if the room has no such state.
//
// It is used for m.room.server_acl, which gates access independently of
// membership.
func (s *Store) GetCurrentStateEventJSON(ctx context.Context, roomID, evType, stateKey string) ([]byte, error) {
	_, raw, err := s.GetCurrentStateEvent(ctx, roomID, evType, stateKey)
	return raw, err
}

// GetCurrentStateEvent returns the ID and JSON of a current state event.
//
// The ID matters as much as the JSON: an event's content is immutable, so
// anything derived from it can be cached against the ID forever, while the
// question of *which* event is current stays a live lookup. That split is what
// lets the server ACL be parsed once without ever serving a stale one.
func (s *Store) GetCurrentStateEvent(ctx context.Context, roomID, evType, stateKey string) (string, []byte, error) {
	const q = `
		SELECT cse.event_id, ej.json
		FROM current_state_events AS cse
		JOIN event_json AS ej USING (event_id)
		WHERE cse.room_id = $1 AND cse.type = $2 AND cse.state_key = $3`

	var eventID string
	var raw []byte
	err := s.pool.QueryRow(ctx, q, roomID, evType, stateKey).Scan(&eventID, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, ErrNotFound
	}
	if err != nil {
		return "", nil, fmt.Errorf("store: get current state event: %w", err)
	}
	return eventID, raw, nil
}

// ServerKeyJSON is a cached published key response for a remote server, as
// Synapse stores it in server_keys_json.
type ServerKeyJSON struct {
	KeyID        string
	FromServer   string
	ValidUntilMS int64
	JSON         []byte
}

// GetServerKeys returns Synapse's cached key responses for a server that are
// still within their validity window, newest first.
//
// This is Synapse's own first-tier key fetcher. Reading it means we resolve
// keys from exactly the data Synapse resolves them from, with no network
// traffic and no cold start after a restart.
func (s *Store) GetServerKeys(ctx context.Context, serverName string, validAtMS int64) ([]ServerKeyJSON, error) {
	const q = `
		SELECT key_id, from_server, ts_valid_until_ms, key_json
		FROM server_keys_json
		WHERE server_name = $1 AND ts_valid_until_ms > $2
		ORDER BY ts_valid_until_ms DESC`

	rows, err := s.pool.Query(ctx, q, serverName, validAtMS)
	if err != nil {
		return nil, fmt.Errorf("store: get server keys: %w", err)
	}
	defer rows.Close()

	var out []ServerKeyJSON
	for rows.Next() {
		var k ServerKeyJSON
		if err := rows.Scan(&k.KeyID, &k.FromServer, &k.ValidUntilMS, &k.JSON); err != nil {
			return nil, fmt.Errorf("store: scan server key: %w", err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: server key rows: %w", err)
	}
	return out, nil
}
