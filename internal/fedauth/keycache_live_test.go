package fedauth

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/store"
)

// TestLiveSynapseKeyCache checks that we can resolve published keys from
// Synapse's own cache, which is the tier that spares us a network fetch for
// every server Synapse has already seen.
func TestLiveSynapseKeyCache(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set; skipping live database test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Sample servers whose cached keys are currently valid.
	rows, err := db.Pool().Query(ctx, `
		SELECT DISTINCT server_name FROM server_keys_json
		WHERE ts_valid_until_ms > $1 LIMIT 40`, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	var servers []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		servers = append(servers, s)
	}
	rows.Close()
	if len(servers) == 0 {
		t.Skip("no currently-valid cached keys")
	}

	cache := newLayeredCache(db, time.Hour, zerolog.Nop())

	var resolved, badSelfSig int
	for _, server := range servers {
		res, err := cache.LoadKeys(server)
		if err != nil {
			t.Errorf("%s: %v", server, err)
			continue
		}
		if res == nil {
			// Every stored response failed validation, which is worth counting
			// but is not necessarily wrong: keys expire and rotate.
			badSelfSig++
			continue
		}
		if res.ServerName != server {
			t.Errorf("%s: resolved a response naming %q", server, res.ServerName)
		}
		if len(res.VerifyKeys) == 0 {
			t.Errorf("%s: resolved a response with no verify keys", server)
		}
		resolved++
	}

	t.Logf("resolved keys for %d of %d servers from Synapse's cache (%d unusable)",
		resolved, len(servers), badSelfSig)

	if resolved == 0 {
		t.Error("resolved no keys at all from Synapse's cache")
	}
}

// TestLiveKeyCachePromotesToMemory checks the second lookup avoids the database.
func TestLiveKeyCachePromotesToMemory(t *testing.T) {
	dsn := os.Getenv("GOPRO_TEST_DSN")
	if dsn == "" {
		t.Skip("GOPRO_TEST_DSN not set; skipping live database test")
	}
	ctx := context.Background()
	db, err := store.Open(ctx, store.Config{DSN: dsn, MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var server string
	if err := db.Pool().QueryRow(ctx, `
		SELECT server_name FROM server_keys_json
		WHERE ts_valid_until_ms > $1 LIMIT 1`, time.Now().UnixMilli()).Scan(&server); err != nil {
		t.Skipf("no valid cached key: %v", err)
	}

	cache := newLayeredCache(db, time.Hour, zerolog.Nop())
	if _, err := cache.LoadKeys(server); err != nil {
		t.Fatal(err)
	}

	// Drop the database so a second hit can only be served from memory.
	db.Close()
	res, err := cache.LoadKeys(server)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Error("second lookup missed; the key was not promoted into memory")
	}
}
