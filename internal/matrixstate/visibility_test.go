package matrixstate

import "testing"

func TestEventVisibleToServer(t *testing.T) {
	const target = "remote.example"
	join := []Membership{{UserID: "@a:remote.example", Membership: MembershipJoin}}
	invite := []Membership{{UserID: "@a:remote.example", Membership: MembershipInvite}}
	leave := []Membership{{UserID: "@a:remote.example", Membership: "leave"}}

	cases := []struct {
		name        string
		vis         string
		memberships []Membership
		erased      map[string]bool
		partial     bool
		want        bool
	}{
		// Open settings do not consult membership at all.
		{"shared is open", HistoryVisibilityShared, nil, nil, false, true},
		{"world_readable is open", HistoryVisibilityWorldReadable, nil, nil, false, true},
		// An unrecognised value is treated as open, as Synapse does: the
		// restrictive settings are enumerated, not the permissive ones.
		{"unknown setting is open", "something_else", nil, nil, false, true},

		{"joined needs a join", HistoryVisibilityJoined, join, nil, false, true},
		{"joined rejects an invite", HistoryVisibilityJoined, invite, nil, false, false},
		{"joined rejects a leave", HistoryVisibilityJoined, leave, nil, false, false},
		{"joined rejects no membership", HistoryVisibilityJoined, nil, nil, false, false},

		{"invited accepts an invite", HistoryVisibilityInvited, invite, nil, false, true},
		{"invited accepts a join", HistoryVisibilityInvited, join, nil, false, true},
		{"invited rejects a leave", HistoryVisibilityInvited, leave, nil, false, false},

		// Erasure and partial state win over everything, including open
		// visibility.
		{"erased sender hidden even when shared", HistoryVisibilityShared, join,
			map[string]bool{"@sender:example.com": true}, false, false},
		{"partial state hidden even when shared", HistoryVisibilityShared, join, nil, true, false},
		{"erasure beats a join", HistoryVisibilityJoined, join,
			map[string]bool{"@sender:example.com": true}, false, false},

		// A user recorded as erased=false must not be hidden.
		{"explicitly not erased", HistoryVisibilityShared, nil,
			map[string]bool{"@sender:example.com": false}, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := eventVisibleToServer("@sender:example.com", target, tc.vis,
				tc.erased, tc.partial, tc.memberships)
			if got != tc.want {
				t.Errorf("visible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEventVisibleToServerFirstMatchingMembershipWins(t *testing.T) {
	// A server can have many users in a room. One join is enough.
	memberships := []Membership{
		{UserID: "@a:remote.example", Membership: "leave"},
		{UserID: "@b:remote.example", Membership: "ban"},
		{UserID: "@c:remote.example", Membership: MembershipJoin},
	}
	if !eventVisibleToServer("@s:example.com", "remote.example",
		HistoryVisibilityJoined, nil, false, memberships) {
		t.Error("a single join among several memberships should make the event visible")
	}
}

func TestDomainOf(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"@user:example.com", "example.com"},
		{"@user:example.com:8448", "example.com:8448"},
		// Synapse splits on the first colon, so a malformed ID yields
		// everything after it.
		{"@a:b:c", "b:c"},
		{"nocolon", ""},
		{"", ""},
	} {
		if got := domainOf(tc.in); got != tc.want {
			t.Errorf("domainOf(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
