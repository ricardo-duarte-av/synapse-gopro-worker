package matrixstate

import "strings"

// History visibility settings, per the Matrix spec.
const (
	HistoryVisibilityWorldReadable = "world_readable"
	HistoryVisibilityShared        = "shared"
	HistoryVisibilityInvited       = "invited"
	HistoryVisibilityJoined        = "joined"
)

// Membership values.
const (
	MembershipJoin   = "join"
	MembershipInvite = "invite"
)

// Membership is one server-local user's membership at an event.
type Membership struct {
	UserID     string
	Membership string
}

// eventVisibleToServer decides whether a remote server may see an event.
//
// Ported from Synapse's Rust event_visible_to_server. This is the only place in
// the worker where a mistake leaks private room history to a server that should
// not have it, so it follows the original exactly rather than being rewritten
// into something that merely looks equivalent.
func eventVisibleToServer(
	sender string,
	targetServer string,
	historyVisibility string,
	erasedSenders map[string]bool,
	partialStateInvisible bool,
	memberships []Membership,
) bool {
	// An erased user's content must not be served at all.
	if erasedSenders[sender] {
		return false
	}
	// During a partial join our membership view is incomplete, so we cannot
	// judge who may see a remote server's events.
	if partialStateInvisible {
		return false
	}

	// Anything other than "invited" or "joined" is open to servers in the room.
	// Note this treats an unrecognised value as open, which is what Synapse
	// does: the restrictive settings are enumerated, not the permissive ones.
	if historyVisibility != HistoryVisibilityInvited && historyVisibility != HistoryVisibilityJoined {
		return true
	}

	for _, m := range memberships {
		switch m.Membership {
		case MembershipInvite:
			if historyVisibility == HistoryVisibilityInvited {
				return true
			}
		case MembershipJoin:
			return true
		}
	}
	return false
}

// domainOf returns the server name from a Matrix ID, using the first colon as
// the separator, matching Synapse.
func domainOf(id string) string {
	idx := strings.Index(id, ":")
	if idx == -1 {
		return ""
	}
	return id[idx+1:]
}
