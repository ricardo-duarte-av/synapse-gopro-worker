package matrixstate

import (
	"encoding/json"
	"testing"
)

func TestApplyPDUJSONRules(t *testing.T) {
	const now = 1000000

	cases := []struct {
		name string
		in   string
		want map[string]any
	}{
		{
			// The case that caused 271 live mismatches: remote servers send
			// "age", Synapse stores it but never echoes it back.
			name: "age is dropped",
			in:   `{"type":"m.room.message","unsigned":{"age":74}}`,
			want: map[string]any{},
		},
		{
			name: "age_ts becomes age",
			in:   `{"type":"m.room.message","unsigned":{"age_ts":999000}}`,
			want: map[string]any{"age": float64(1000)},
		},
		{
			// A stored age must not survive alongside a computed one.
			name: "stored age is replaced by the computed one",
			in:   `{"type":"m.room.message","unsigned":{"age":74,"age_ts":999000}}`,
			want: map[string]any{"age": float64(1000)},
		},
		{
			name: "allowlisted fields survive",
			in: `{"type":"m.room.member","unsigned":{"replaces_state":"$old",` +
				`"prev_sender":"@a:b","prev_content":{"membership":"join"}}}`,
			want: map[string]any{
				"replaces_state": "$old",
				"prev_sender":    "@a:b",
				"prev_content":   map[string]any{"membership": "join"},
			},
		},
		{
			name: "non-allowlisted fields are dropped",
			in: `{"type":"m.room.message","unsigned":{"redacted_by":"$r",` +
				`"redacted_because":{"x":1},"something_new":true,"replaces_state":"$old"}}`,
			want: map[string]any{"replaces_state": "$old"},
		},
		{
			// Synapse serialises its typed struct unconditionally, so unsigned
			// is present even when there is nothing in it.
			name: "unsigned is emitted when absent",
			in:   `{"type":"m.room.message"}`,
			want: map[string]any{},
		},
		{
			name: "invite and knock room state survive",
			in:   `{"type":"m.room.member","unsigned":{"invite_room_state":[{"a":1}],"knock_room_state":[]}}`,
			want: map[string]any{
				"invite_room_state": []any{map[string]any{"a": float64(1)}},
				"knock_room_state":  []any{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := applyPDUJSONRules([]byte(tc.in), now)
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Unsigned map[string]any `json:"unsigned"`
			}
			if err := json.Unmarshal(out, &got); err != nil {
				t.Fatal(err)
			}
			if got.Unsigned == nil {
				t.Fatal("unsigned is absent; Synapse always emits it")
			}
			if len(got.Unsigned) != len(tc.want) {
				t.Errorf("unsigned = %v, want %v", got.Unsigned, tc.want)
				return
			}
			gotJSON, _ := json.Marshal(got.Unsigned)
			wantJSON, _ := json.Marshal(tc.want)
			if string(gotJSON) != string(wantJSON) {
				t.Errorf("unsigned = %s, want %s", gotJSON, wantJSON)
			}
		})
	}
}

func TestApplyPDUJSONRulesPreservesEventBody(t *testing.T) {
	// Everything outside unsigned must be untouched: the signature covers it.
	in := `{"type":"m.room.message","room_id":"!r:x","sender":"@a:b",` +
		`"content":{"body":"hi"},"signatures":{"x":{"ed25519:a":"sig"}},` +
		`"hashes":{"sha256":"h"},"unsigned":{"age":5}}`
	out, err := applyPDUJSONRules([]byte(in), 1000)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"type", "room_id", "sender", "content", "signatures", "hashes"} {
		if _, ok := got[field]; !ok {
			t.Errorf("field %q was lost", field)
		}
	}
	sigs, _ := json.Marshal(got["signatures"])
	if string(sigs) != `{"x":{"ed25519:a":"sig"}}` {
		t.Errorf("signatures were altered: %s", sigs)
	}
}
