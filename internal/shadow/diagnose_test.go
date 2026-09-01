package shadow

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/daedric/synapse-gopro-worker/internal/matrixstate"
)

// stateBody builds a /state response from raw PDU JSON.
func stateBody(pdus, authChain []string) string {
	return `{"pdus":[` + strings.Join(pdus, ",") + `],"auth_chain":[` + strings.Join(authChain, ",") + `]}`
}

func replayerFor(body string) StateReplayer {
	return func(_ context.Context, _ Request, w io.Writer) error {
		_, err := io.WriteString(w, body)
		return err
	}
}

func member(user, membership string, ts int64) string {
	return fmt.Sprintf(
		`{"type":"m.room.member","state_key":%q,"sender":%q,"content":{"membership":%q},`+
			`"origin_server_ts":%d,"depth":7,"room_id":"!r:example.com","unsigned":{}}`,
		user, user, membership, ts)
}

func diagnose(t *testing.T, ours, theirs string, samples int) *StateDiagnosis {
	t.Helper()
	d, err := DiagnoseStateMismatch(context.Background(), Request{Endpoint: "state"},
		replayerFor(ours), replayerFor(theirs), samples)
	if err != nil {
		t.Fatalf("diagnose: %v", err)
	}
	return d
}

func TestDiagnoseIdenticalIsEmpty(t *testing.T) {
	body := stateBody([]string{member("@a:e.com", "join", 1)}, []string{member("@b:e.com", "join", 2)})
	if d := diagnose(t, body, body, 0); !d.Empty() {
		t.Fatalf("identical bodies reported a difference: %+v", d)
	}
}

// Key order must not be a difference: we splice stored JSON while Synapse
// re-serialises from a dict, so the two disagree on ordering almost everywhere.
func TestDiagnoseIgnoresKeyOrder(t *testing.T) {
	ours := stateBody([]string{
		`{"type":"m.room.member","state_key":"@a:e.com","sender":"@a:e.com","content":{"membership":"join"},"depth":7}`,
	}, nil)
	theirs := stateBody([]string{
		`{"depth":7,"content":{"membership":"join"},"sender":"@a:e.com","state_key":"@a:e.com","type":"m.room.member"}`,
	}, nil)
	if d := diagnose(t, ours, theirs, 0); !d.Empty() {
		t.Fatalf("key order reported as a difference: %+v", d)
	}
}

func TestDiagnoseNamesChangedContent(t *testing.T) {
	ours := stateBody([]string{member("@a:e.com", "join", 1)}, nil)
	theirs := stateBody([]string{member("@a:e.com", "leave", 1)}, nil)

	d := diagnose(t, ours, theirs, 0)
	if d.PDUs.NativeOnly != 1 || d.PDUs.SynapseOnly != 1 {
		t.Fatalf("want 1 each way, got native=%d synapse=%d", d.PDUs.NativeOnly, d.PDUs.SynapseOnly)
	}
	// The same (type, state_key) on both sides is what says "content differs"
	// rather than "the set differs", and is the whole point of naming them.
	if len(d.PDUs.NativeSamples) != 1 || len(d.PDUs.SynapseSamples) != 1 {
		t.Fatalf("missing samples: %+v", d.PDUs)
	}
	got, want := d.PDUs.NativeSamples[0], d.PDUs.SynapseSamples[0]
	if got.Type != "m.room.member" || got.StateKey != "@a:e.com" {
		t.Errorf("native sample not identified: %+v", got)
	}
	if want.StateKey != got.StateKey {
		t.Errorf("same event should appear on both sides: %+v vs %+v", got, want)
	}
}

func TestDiagnoseNamesExtraAndMissing(t *testing.T) {
	shared := member("@a:e.com", "join", 1)
	extra := member("@extra:e.com", "join", 2)
	missing := member("@missing:e.com", "join", 3)

	d := diagnose(t, stateBody([]string{shared, extra}, nil), stateBody([]string{shared, missing}, nil), 0)
	if d.PDUs.NativeOnly != 1 || d.PDUs.SynapseOnly != 1 {
		t.Fatalf("want 1/1, got %+v", d.PDUs)
	}
	if d.PDUs.NativeSamples[0].StateKey != "@extra:e.com" {
		t.Errorf("native-only event misidentified: %+v", d.PDUs.NativeSamples[0])
	}
	if d.PDUs.SynapseSamples[0].StateKey != "@missing:e.com" {
		t.Errorf("synapse-only event misidentified: %+v", d.PDUs.SynapseSamples[0])
	}
}

// The two arrays must be diagnosed apart. A single residue would let an event
// in the wrong array cancel against the right one and report nothing -- which
// is exactly the failure the two-digest design exists to prevent.
func TestDiagnoseSeparatesTheTwoArrays(t *testing.T) {
	ev := member("@a:e.com", "join", 1)
	d := diagnose(t, stateBody([]string{ev}, nil), stateBody(nil, []string{ev}), 0)
	if d.PDUs.NativeOnly != 1 {
		t.Errorf("pdus: want 1 native-only, got %+v", d.PDUs)
	}
	if d.AuthChain.SynapseOnly != 1 {
		t.Errorf("auth_chain: want 1 synapse-only, got %+v", d.AuthChain)
	}
}

// Counts stay exact when the naming is capped; a systematic fault must not
// look small just because only a few events were named.
func TestDiagnoseCountsExactWhenSamplesCapped(t *testing.T) {
	var ours []string
	for i := 0; i < 50; i++ {
		ours = append(ours, member(fmt.Sprintf("@u%d:e.com", i), "join", int64(i)))
	}
	d := diagnose(t, stateBody(ours, nil), stateBody(nil, nil), 3)
	if d.PDUs.NativeOnly != 50 {
		t.Errorf("count should be exact: got %d want 50", d.PDUs.NativeOnly)
	}
	if len(d.PDUs.NativeSamples) != 3 {
		t.Errorf("samples should be capped at 3, got %d", len(d.PDUs.NativeSamples))
	}
}

// An event repeated in one array is a real difference in multiplicity, and the
// residue must survive cancelling to say so.
func TestDiagnoseCountsMultiplicity(t *testing.T) {
	ev := member("@a:e.com", "join", 1)
	d := diagnose(t, stateBody([]string{ev, ev, ev}, nil), stateBody([]string{ev}, nil), 0)
	if d.PDUs.NativeOnly != 2 || d.PDUs.SynapseOnly != 0 {
		t.Fatalf("want 2 native-only, got %+v", d.PDUs)
	}
	if len(d.PDUs.NativeSamples) != 2 {
		t.Errorf("both surplus copies should be named, got %d", len(d.PDUs.NativeSamples))
	}
}

func TestDiagnoseRequiresBothSides(t *testing.T) {
	if _, err := DiagnoseStateMismatch(context.Background(), Request{}, replayerFor("{}"), nil, 0); err == nil {
		t.Fatal("a diagnosis with one side should be refused, not guessed")
	}
}

func TestDiagnoseReportsUnparseableSide(t *testing.T) {
	_, err := DiagnoseStateMismatch(context.Background(), Request{},
		replayerFor(`{"pdus":[{"type":"m.room.member"`), replayerFor(stateBody(nil, nil)), 0)
	if err == nil {
		t.Fatal("a truncated response should be an error, not an empty diagnosis")
	}
}

// 14,654 events here carry an escaped NUL, which PostgreSQL's jsonb cannot even
// cast. A diagnosis that choked on one would fail exactly where it is least
// welcome, so the scan must carry it through untouched.
func TestDiagnoseHandlesEscapedNUL(t *testing.T) {
	ev := `{"type":"m.room.message","sender":"@a:e.com","content":{"body":"x\u0000y"},"depth":3}`
	body := stateBody([]string{ev}, nil)
	if d := diagnose(t, body, body, 0); !d.Empty() {
		t.Fatalf("escaped NUL reported as a difference: %+v", d)
	}
	d := diagnose(t, body, stateBody(nil, nil), 0)
	if d.PDUs.NativeOnly != 1 {
		t.Fatalf("escaped-NUL event not counted: %+v", d.PDUs)
	}
	if d.PDUs.NativeSamples[0].Type != "m.room.message" {
		t.Fatalf("escaped-NUL event not identified: %+v", d.PDUs.NativeSamples[0])
	}
}

// TestRunnerDiagnosesOnMismatch checks the hook is actually reached.
//
// The mechanism being correct is not the same as it being called: the /state
// mismatch path is exercised roughly never, so a diagnosis wired to nothing
// would look exactly like a diagnosis that works until the day it matters.
func TestRunnerDiagnosesOnMismatch(t *testing.T) {
	r := NewRunner(nil, "example.com", nil, nil, zerolog.Nop(), Options{Concurrency: 1})

	ours := stateBody([]string{member("@a:e.com", "join", 1)}, nil)
	theirs := stateBody([]string{member("@a:e.com", "leave", 1)}, nil)

	called := make(chan string, 8)
	r.SetStateReplayers(
		func(_ context.Context, _ Request, w io.Writer) error {
			called <- "native"
			_, err := io.WriteString(w, ours)
			return err
		},
		func(_ context.Context, hr *http.Request, w io.Writer) error {
			called <- "synapse"
			if hr.URL.RequestURI() != "/_matrix/federation/v1/state/%21r:e.com" {
				t.Errorf("upstream replay lost the URI: %q", hr.URL.RequestURI())
			}
			_, err := io.WriteString(w, theirs)
			return err
		},
	)

	r.diagnoseStateMismatch(Request{
		Endpoint: "state",
		URI:      "/_matrix/federation/v1/state/%21r:e.com",
		Method:   http.MethodGet,
	})

	// Both sides are read twice: once to find the residue, once to name it.
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		select {
		case s := <-called:
			seen[s]++
		case <-time.After(5 * time.Second):
			t.Fatalf("diagnosis did not replay both sides twice; saw %v", seen)
		}
	}
	if seen["native"] != 2 || seen["synapse"] != 2 {
		t.Fatalf("want two passes over each side, got %v", seen)
	}
}

// A Runner with no replayers must stay silent rather than panic: the diagnosis
// is optional, and a /state mismatch is already bad enough without the
// reporting of it crashing the comparison goroutine.
func TestRunnerWithoutReplayersDoesNotDiagnose(t *testing.T) {
	r := NewRunner(nil, "example.com", nil, nil, zerolog.Nop(), Options{Concurrency: 1})
	r.diagnoseStateMismatch(Request{Endpoint: "state"})
}

// TestCompareStateServedTriggersDiagnosis drives the real comparison path.
//
// The test above calls diagnoseStateMismatch directly, which proves the
// mechanism and nothing about whether anything reaches it. This one hands
// CompareStateServed two disagreeing digests and asserts the replayers fire,
// so deleting the call from the mismatch branch fails the test.
func TestCompareStateServedTriggersDiagnosis(t *testing.T) {
	r := NewRunner(nil, "example.com", nil, nil, zerolog.Nop(), Options{Concurrency: 1})

	ours := stateBody([]string{member("@a:e.com", "join", 1)}, nil)
	theirs := stateBody([]string{member("@a:e.com", "leave", 1)}, nil)

	fired := make(chan struct{}, 8)
	r.SetStateReplayers(
		func(_ context.Context, _ Request, w io.Writer) error {
			fired <- struct{}{}
			_, err := io.WriteString(w, ours)
			return err
		},
		func(_ context.Context, _ *http.Request, w io.Writer) error {
			fired <- struct{}{}
			_, err := io.WriteString(w, theirs)
			return err
		},
	)

	// Two answers that disagree. The digests are what the comparison reads;
	// the bodies above are what the diagnosis will then go and fetch.
	mine := matrixstate.StateResult{PDUs: 1, PDUDigest: [32]byte{1}}
	synapse := matrixstate.StateResult{PDUs: 1, PDUDigest: [32]byte{2}}

	r.CompareStateServed(
		Request{Endpoint: "state", URI: "/_matrix/federation/v1/state/%21r:e.com", Method: http.MethodGet},
		ProxyResult{Status: http.StatusOK},
		synapse, nil, mine, time.Millisecond,
	)

	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("a served /state mismatch did not trigger the diagnosis")
	}
}

// The agreeing case must not trigger it: four streamed responses per matching
// comparison would be a self-inflicted load problem on the busiest path.
func TestCompareStateServedDoesNotDiagnoseOnMatch(t *testing.T) {
	r := NewRunner(nil, "example.com", nil, nil, zerolog.Nop(), Options{Concurrency: 1})

	fired := make(chan struct{}, 8)
	r.SetStateReplayers(
		func(_ context.Context, _ Request, w io.Writer) error { fired <- struct{}{}; return nil },
		func(_ context.Context, _ *http.Request, w io.Writer) error { fired <- struct{}{}; return nil },
	)

	same := matrixstate.StateResult{PDUs: 1, PDUDigest: [32]byte{7}}
	r.CompareStateServed(
		Request{Endpoint: "state", URI: "/x", Method: http.MethodGet},
		ProxyResult{Status: http.StatusOK},
		same, nil, same, time.Millisecond,
	)

	select {
	case <-fired:
		t.Fatal("agreeing answers should not be diagnosed")
	case <-time.After(250 * time.Millisecond):
	}
}
