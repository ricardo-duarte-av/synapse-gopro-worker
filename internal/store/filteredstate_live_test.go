package store

import (
	"context"
	"maps"
	"os"
	"testing"

	"github.com/daedric/synapse-gopro-worker/internal/cache"
)

// TestLiveFilteredStateMatchesDatabase is the check this optimisation lives or
// dies by: filtering a cached whole map must produce byte-identical results to
// the SQL walk it replaces. It runs against real state groups, including the
// large ones, because that is where a filter mistake would hide.
func TestLiveFilteredStateMatchesDatabase(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set")
	}
	ctx := context.Background()
	db, err := Open(ctx, Config{DSN: dsn, Cache: cacheSettingsForTest()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// A spread of real state groups: small rooms, big rooms, recent activity.
	rows, err := db.pool.Query(ctx, `
		SELECT state_group FROM (
			SELECT state_group, count(*) AS n
			FROM state_groups_state GROUP BY state_group
			ORDER BY n DESC LIMIT 15
		) big
		UNION
		SELECT state_group FROM event_to_state_groups
		ORDER BY 1 DESC LIMIT 30`)
	if err != nil {
		t.Fatal(err)
	}
	var groups []int64
	for rows.Next() {
		var g int64
		if err := rows.Scan(&g); err != nil {
			t.Fatal(err)
		}
		groups = append(groups, g)
	}
	rows.Close()
	if len(groups) == 0 {
		t.Skip("no state groups")
	}

	const server = "matrix.org"
	types := []string{"m.room.history_visibility"}
	var compared int

	// There are three sources now and all three must agree: the SQL walk, the
	// whole-map cache filtered in memory, and the filtered cache itself. The
	// purges matter — without them a later call just replays the first
	// answer and the comparison is vacuous.
	for _, g := range groups {
		// Ground truth: the raw SQL walks, bypassing every cache. Comparing
		// the cached paths only against each other would pass even when all
		// of them are wrong in the same way -- which is exactly what a cache
		// key collision looks like.
		wantTypes, err := db.filteredState(ctx, stateGroupFilteredQuery, g, types, nil)
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}
		wantMembers, err := db.filteredState(ctx, stateGroupMemberQuery, g, nil, ptr("%:"+server))
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}

		// 1. Straight from the database, every cache cold.
		db.PurgeCaches()
		fromDBTypes, err := db.GetFilteredStateForGroup(ctx, g, types)
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}
		fromDBMembers, err := db.GetServerMembershipStateForGroup(ctx, g, server)
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}

		// 2. Filtered from the whole map: drop the filtered answers, warm the
		//    whole map, ask again.
		db.caches.filteredState.Purge()
		if _, err := db.GetStateForGroup(ctx, g); err != nil {
			t.Fatalf("group %d: %v", g, err)
		}
		fromWholeTypes, err := db.GetFilteredStateForGroup(ctx, g, types)
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}
		fromWholeMembers, err := db.GetServerMembershipStateForGroup(ctx, g, server)
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}

		// 3. From the filtered cache, which step 2 just populated.
		fromFilteredTypes, err := db.GetFilteredStateForGroup(ctx, g, types)
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}
		fromFilteredMembers, err := db.GetServerMembershipStateForGroup(ctx, g, server)
		if err != nil {
			t.Fatalf("group %d: %v", g, err)
		}

		for _, c := range []struct {
			what string
			a, b map[StateKey]string
		}{
			{"types via database", wantTypes, fromDBTypes},
			{"types via whole map", wantTypes, fromWholeTypes},
			{"types via filtered cache", wantTypes, fromFilteredTypes},
			{"members via database", wantMembers, fromDBMembers},
			{"members via whole map", wantMembers, fromWholeMembers},
			{"members via filtered cache", wantMembers, fromFilteredMembers},
		} {
			if !maps.Equal(c.a, c.b) {
				t.Errorf("group %d: %s differs (%d vs %d entries)\n  %v\n  %v",
					g, c.what, len(c.a), len(c.b), c.a, c.b)
			}
		}
		compared++
	}
	t.Logf("compared all three sources across %d real state groups", compared)
}

// The filtered result must never alias the cached map: adjusting it in place
// has been a correctness bug here twice already.
func TestLiveFilteredStateReturnsACopy(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set")
	}
	ctx := context.Background()
	db, err := Open(ctx, Config{DSN: dsn, Cache: cacheSettingsForTest()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var group int64
	if err := db.pool.QueryRow(ctx,
		`SELECT state_group FROM event_to_state_groups ORDER BY 1 DESC LIMIT 1`).Scan(&group); err != nil {
		t.Skip("no state groups")
	}
	full, err := db.GetStateForGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	before := len(full)

	filtered, err := db.GetFilteredStateForGroup(ctx, group, []string{"m.room.member"})
	if err != nil {
		t.Fatal(err)
	}
	for k := range filtered {
		delete(filtered, k)
	}
	again, err := db.GetStateForGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != before {
		t.Errorf("cached map lost entries: %d -> %d; the filter aliased the cache", before, len(again))
	}
}

// cacheSettingsForTest sizes the caches generously so a large state group is
// admitted rather than refused, which would silently skip the cached path.
func cacheSettingsForTest() cache.Settings {
	return cache.Settings{
		StateGroupsMB:      1024,
		EventsMB:           128,
		EventStateGroupsMB: 32,
		AuthChainsMB:       64,
	}
}
