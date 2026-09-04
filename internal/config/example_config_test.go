package config

import "testing"

// The shipped example config must actually load. It went unchecked long enough
// to grow a `verify_wait_seconds` under `auth:`, where the loader does not
// look -- so anyone starting from it got a startup error on an otherwise
// correct file.
func TestExampleConfigLoads(t *testing.T) {
	c, err := Load("../../deploy/gopro-worker.example.yaml")
	if err != nil {
		t.Fatalf("example config does not load: %v", err)
	}
	if err := c.validate(); err != nil {
		t.Fatalf("example config does not validate: %v", err)
	}
	if c.Upstream.TimeoutSeconds != 180 {
		t.Errorf("upstream.timeout_seconds = %d, want 180: Synapse needs 82s for the largest room", c.Upstream.TimeoutSeconds)
	}
	if c.Shadow.VerifyWaitSeconds == 0 {
		t.Error("shadow.verify_wait_seconds did not reach the Shadow struct")
	}
}
