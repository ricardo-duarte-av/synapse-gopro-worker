package matrixstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"maunium.net/go/mautrix/federation/pdu"
	"maunium.net/go/mautrix/id"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

const historyVisibilityType = "m.room.history_visibility"

// TransactionResponse is the body of GET /_matrix/federation/v1/event, which
// the spec shapes as a transaction carrying a single PDU.
type TransactionResponse struct {
	Origin         string            `json:"origin"`
	OriginServerTS int64             `json:"origin_server_ts"`
	PDUs           []json.RawMessage `json:"pdus"`
}

// ErrEventNotFound signals that we do not have the event. Synapse answers this
// with a 404 and an empty body rather than a JSON error, so it is distinct from
// a MatrixError.
var ErrEventNotFound = errors.New("event not found")

// Event answers /event for a remote server.
//
// Unlike the state endpoints, this one applies history-visibility filtering:
// an event the server may not see is returned redacted rather than withheld.
// It is also the only one of the three where a mistake leaks private room
// history, so every step mirrors Synapse's get_persisted_pdu.
func (r *Resolver) Event(ctx context.Context, origin, serverName, eventID string) (*TransactionResponse, error) {
	ev, err := r.db.GetEvent(ctx, eventID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrEventNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get event: %w", err)
	}

	// Note that Synapse checks room membership here but, unlike the state
	// endpoints, does not check the room's server ACL. We mirror that.
	partial, err := r.db.IsPartialStateRoom(ctx, ev.RoomID)
	if err != nil {
		return nil, fmt.Errorf("partial state check: %w", err)
	}
	if partial {
		return nil, errPartialState()
	}
	inRoom, err := r.db.IsHostInRoom(ctx, ev.RoomID, origin)
	if err != nil {
		return nil, fmt.Errorf("membership check: %w", err)
	}
	if !inRoom {
		return nil, errHostNotInRoom()
	}

	visible, err := r.eventVisible(ctx, ev, origin, serverName)
	if err != nil {
		return nil, err
	}

	// An event that has been redacted is served in redacted form regardless of
	// visibility: redaction is how a user deletes a message. Synapse applies
	// this when loading the event, before any visibility filtering, so a
	// redacted event is already pruned by the time visibility is considered.
	body := ev.JSON
	if ev.IsRedacted() || !visible {
		body, err = redactEvent(ev)
		if err != nil {
			return nil, fmt.Errorf("redact event: %w", err)
		}
	}

	now := time.Now().UnixMilli()
	body, err = applyPDUJSONRules(body, now)
	if err != nil {
		return nil, fmt.Errorf("prepare pdu: %w", err)
	}

	return &TransactionResponse{
		Origin:         serverName,
		OriginServerTS: now,
		PDUs:           []json.RawMessage{body},
	}, nil
}

// eventVisible applies Synapse's filter_events_for_server to a single event.
func (r *Resolver) eventVisible(ctx context.Context, ev *store.Event, targetServer, localServer string) (bool, error) {
	sender, err := senderOf(ev.JSON)
	if err != nil {
		return false, err
	}

	erased, err := r.db.AreUsersErased(ctx, []string{sender})
	if err != nil {
		return false, fmt.Errorf("erased users: %w", err)
	}

	// A remote server's events in a partially-joined room are invisible; the
	// room is not partial-stated here, so this is always false at this point,
	// but it is kept explicit to match the original.
	partialStateInvisible := false

	vis, memberships, err := r.visibilityAt(ctx, ev, targetServer)
	if err != nil {
		return false, err
	}
	return eventVisibleToServer(sender, targetServer, vis, erased, partialStateInvisible, memberships), nil
}

// visibilityAt resolves the history visibility setting at an event, and the
// target server's memberships there if the setting requires them.
//
// The membership lookup is skipped whenever visibility is open, which is the
// common case. That matters: pulling a large room's membership list for every
// /event request would be far more expensive than the lookup it guards.
func (r *Resolver) visibilityAt(ctx context.Context, ev *store.Event, targetServer string) (string, []Membership, error) {
	if ev.Outlier {
		// We have no state at an outlier. Synapse treats that as no
		// history_visibility event, which means open.
		return HistoryVisibilityShared, nil, nil
	}

	group, err := r.db.GetStateGroupForEvent(ctx, ev.EventID)
	if errors.Is(err, store.ErrNotFound) {
		return HistoryVisibilityShared, nil, nil
	}
	if err != nil {
		return "", nil, fmt.Errorf("state group: %w", err)
	}

	state, err := r.db.GetFilteredStateForGroup(ctx, group, []string{historyVisibilityType})
	if err != nil {
		return "", nil, fmt.Errorf("history visibility state: %w", err)
	}

	vis := HistoryVisibilityShared
	if visEventID, ok := state[store.StateKey{Type: historyVisibilityType, StateKey: ""}]; ok {
		visEvent, err := r.db.GetEvent(ctx, visEventID)
		if err == nil {
			if v := historyVisibilityOf(visEvent.JSON); v != "" {
				vis = v
			}
		}
	}

	if vis != HistoryVisibilityInvited && vis != HistoryVisibilityJoined {
		return vis, nil, nil
	}

	memberState, err := r.db.GetServerMembershipStateForGroup(ctx, group, targetServer)
	if err != nil {
		return "", nil, fmt.Errorf("membership state: %w", err)
	}

	ids := make([]string, 0, len(memberState))
	keyOf := make(map[string]string, len(memberState))
	for key, id := range memberState {
		// The SQL suffix match is a prefilter; check the domain exactly.
		if domainOf(key.StateKey) != targetServer {
			continue
		}
		ids = append(ids, id)
		keyOf[id] = key.StateKey
	}
	if len(ids) == 0 {
		return vis, nil, nil
	}

	events, err := r.db.GetEvents(ctx, ids)
	if err != nil {
		return "", nil, fmt.Errorf("membership events: %w", err)
	}
	memberships := make([]Membership, 0, len(events))
	for evID, memberEvent := range events {
		memberships = append(memberships, Membership{
			UserID:     keyOf[evID],
			Membership: membershipOf(memberEvent.JSON),
		})
	}
	return vis, memberships, nil
}

// redactEvent strips an event to the fields the room version preserves.
//
// Room versions 1 and 2 use a different event format: auth_events and
// prev_events are arrays of [event_id, hashes] pairs rather than plain event
// IDs, so they need mautrix's RoomV1PDU. Decoding one of those into the modern
// PDU type fails outright. These rooms are rare -- 9 of 1165 on the deployment
// this was written against -- but they federate like any other.
func redactEvent(ev *store.Event) ([]byte, error) {
	roomVersion := id.RoomVersion(ev.RoomVersion)
	if roomVersion == "" {
		// A missing room version means the original, which is format v1.
		roomVersion = id.RoomV1
	}

	// Parsed from a padding-normalised copy, and the original hashes spliced
	// back afterwards.
	//
	// mautrix types hashes.sha256 as jsonbytes.UnpaddedBytes, so a padded
	// value fails to decode and the whole event becomes unredactable. The spec
	// says unpadded, but a remote server sent padded and Synapse stored and
	// serves it verbatim -- eight redacted events here carry one. Refusing
	// them would mean an event we simply cannot answer, which matters most
	// once there is no proxy left to fall back to.
	//
	// The stored bytes are never altered on the way out: redaction preserves
	// hashes unchanged, so whatever was stored is restored over the result.
	source, padded := unpadEventHash(ev.JSON)

	var redacted []byte
	var err error
	if usesV1EventFormat(roomVersion) {
		var p pdu.RoomV1PDU
		if err := json.Unmarshal(source, &p); err != nil {
			return nil, fmt.Errorf("parse v1 pdu: %w", err)
		}
		redacted, err = json.Marshal(p.Redact(roomVersion))
	} else {
		var p pdu.PDU
		if err := json.Unmarshal(source, &p); err != nil {
			return nil, fmt.Errorf("parse pdu: %w", err)
		}
		redacted, err = json.Marshal(p.Redact(roomVersion))
	}
	if err != nil {
		return nil, fmt.Errorf("encode redacted pdu: %w", err)
	}
	if padded != nil {
		// Redaction keeps hashes untouched, so the served event must carry the
		// bytes Synapse stored, padding and all.
		redacted, err = sjson.SetRawBytes(redacted, "hashes", padded)
		if err != nil {
			return nil, fmt.Errorf("restore hashes: %w", err)
		}
	}
	return restorePrunedUnsigned(ev.JSON, redacted)
}

// unpadEventHash returns the event with hashes.sha256 stripped of base64
// padding, and the original hashes object when it had to change.
//
// Returns the input untouched, and a nil original, when nothing needed doing --
// which is every event but a handful.
func unpadEventHash(body []byte) (source []byte, originalHashes []byte) {
	hashes := gjson.GetBytes(body, "hashes")
	if !hashes.Exists() {
		return body, nil
	}
	sha := gjson.GetBytes(body, "hashes.sha256")
	if !sha.Exists() || !strings.HasSuffix(sha.Str, "=") {
		return body, nil
	}
	fixed, err := sjson.SetBytes(body, "hashes.sha256", strings.TrimRight(sha.Str, "="))
	if err != nil {
		return body, nil
	}
	return fixed, []byte(hashes.Raw)
}

// prunedUnsignedFields are the unsigned keys that survive redaction.
//
// mautrix's Redact drops unsigned wholesale, but Synapse's prune_event_dict
// rebuilds it and copies these two across, so a redacted event keeps its age
// and the ID of the state event it replaced. Dropping them served "unsigned":
// {} where Synapse served replaces_state.
var prunedUnsignedFields = []string{"age_ts", "replaces_state"}

// restorePrunedUnsigned copies the fields Synapse keeps through redaction from
// the stored event onto the redacted one.
func restorePrunedUnsigned(original, redacted []byte) ([]byte, error) {
	var ev struct {
		Unsigned map[string]json.RawMessage `json:"unsigned"`
	}
	if err := json.Unmarshal(original, &ev); err != nil {
		return nil, fmt.Errorf("parse stored unsigned: %w", err)
	}

	unsigned := map[string]json.RawMessage{}
	for _, field := range prunedUnsignedFields {
		if v, ok := ev.Unsigned[field]; ok {
			unsigned[field] = v
		}
	}

	encoded, err := json.Marshal(unsigned)
	if err != nil {
		return nil, err
	}
	return sjson.SetRawBytes(redacted, "unsigned", encoded)
}

// usesV1EventFormat reports whether a room version uses the original event
// format. It mirrors RoomV1PDU.SupportsRoomVersion.
func usesV1EventFormat(roomVersion id.RoomVersion) bool {
	switch roomVersion {
	case id.RoomV0, id.RoomV1, id.RoomV2:
		return true
	default:
		return false
	}
}

// persistedUnsignedFields are the only keys Synapse emits inside unsigned.
//
// Synapse models unsigned as a typed struct, so serialising an event keeps
// exactly these fields and silently drops everything else -- including "age",
// which remote servers send and Synapse stores but never echoes back. Passing
// the stored unsigned through verbatim differs from Synapse on any event that
// arrived with extra keys, which in practice is most of them.
var persistedUnsignedFields = []string{
	"age_ts",
	"replaces_state",
	"invite_room_state",
	"knock_room_state",
	"prev_content",
	"prev_sender",
}

// applyPDUJSONRules reshapes a stored event into what Synapse puts on the wire.
//
// unsigned is rebuilt from the allowlist above, then age_ts is converted into
// an age relative to now, mirroring Synapse's get_pdu_json. Because age is
// derived here rather than copied, it is wall-clock dependent and cannot be
// compared between implementations by value.
//
// unsigned is always emitted, even when empty: Synapse serialises its typed
// struct unconditionally, so an event with nothing to report still carries
// "unsigned": {}.
func applyPDUJSONRules(body []byte, nowMS int64) ([]byte, error) {
	return applyPDUJSONRulesAt(body, &nowMS)
}

// applyPDUJSONRulesAt is applyPDUJSONRules with an optional clock, mirroring
// get_pdu_json's Option<i64> time_now.
//
// A nil clock skips the age_ts -> age conversion entirely, keeping age_ts as
// stored. This is not a refinement: /state reaches get_pdu_json through
// serialize_and_filter_pdus, which passes time_now=None, while /event reaches
// it through _transaction_dict_from_pdus, which passes a real clock. So the
// two endpoints serialise the same event differently, and roughly half the
// events here carry age_ts. redacted_because is dropped either way, since it
// is not part of the unsigned allowlist.
func applyPDUJSONRulesAt(body []byte, nowMS *int64) ([]byte, error) {
	var ev struct {
		Unsigned map[string]json.RawMessage `json:"unsigned"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, err
	}

	unsigned := map[string]json.RawMessage{}
	for _, field := range persistedUnsignedFields {
		if v, ok := ev.Unsigned[field]; ok {
			unsigned[field] = v
		}
	}

	if raw, ok := unsigned["age_ts"]; ok && nowMS != nil {
		var ageTS int64
		if err := json.Unmarshal(raw, &ageTS); err == nil {
			age, err := json.Marshal(*nowMS - ageTS)
			if err != nil {
				return nil, err
			}
			unsigned["age"] = age
			delete(unsigned, "age_ts")
		}
	}

	encoded, err := json.Marshal(unsigned)
	if err != nil {
		return nil, err
	}
	return sjson.SetRawBytes(body, "unsigned", encoded)
}

func senderOf(body []byte) (string, error) {
	var ev struct {
		Sender string `json:"sender"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		return "", fmt.Errorf("parse sender: %w", err)
	}
	return ev.Sender, nil
}

func historyVisibilityOf(body []byte) string {
	var ev struct {
		Content struct {
			HistoryVisibility string `json:"history_visibility"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		return ""
	}
	return ev.Content.HistoryVisibility
}

func membershipOf(body []byte) string {
	var ev struct {
		Content struct {
			Membership string `json:"membership"`
		} `json:"content"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		return ""
	}
	return ev.Content.Membership
}
