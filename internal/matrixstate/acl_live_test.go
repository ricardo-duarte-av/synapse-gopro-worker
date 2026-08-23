package matrixstate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// TestLiveServerACLs parses every m.room.server_acl event in the database.
//
// Real ACL lists contain hostile and malformed entries — URLs, host:port
// strings, very long garbage — because anyone in a room can set them. The
// parser must survive all of it, since a failure here would either lock every
// server out of a room or let every server in.
func TestLiveServerACLs(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set; skipping live database test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	s, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	rows, err := s.Pool().Query(ctx, `
		SELECT cse.room_id, ej.json
		FROM current_state_events AS cse
		JOIN event_json AS ej USING (event_id)
		WHERE cse.type = 'm.room.server_acl' AND cse.state_key = ''`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var total, failed, allowsUs, denyAll int
	for rows.Next() {
		var roomID string
		var raw []byte
		if err := rows.Scan(&roomID, &raw); err != nil {
			t.Fatal(err)
		}
		total++

		acl, err := ParseServerACL(raw)
		if err != nil {
			failed++
			if failed <= 3 {
				t.Errorf("failed to parse ACL for %s: %v", roomID, err)
			}
			continue
		}
		if acl.Allowed("aguiarvieira.pt") {
			allowsUs++
		}
		// An ACL that denies a plainly innocuous server suggests an
		// allow-list-of-nothing, which is worth knowing about.
		if !acl.Allowed("matrix.org") && !acl.Allowed("aguiarvieira.pt") {
			denyAll++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if total == 0 {
		t.Skip("no server ACL events in this database")
	}
	t.Logf("parsed %d real ACL events: %d failed, %d allow us, %d appear to deny broadly",
		total, failed, allowsUs, denyAll)

	if failed > 0 {
		t.Errorf("%d of %d real ACL events failed to parse", failed, total)
	}
}
