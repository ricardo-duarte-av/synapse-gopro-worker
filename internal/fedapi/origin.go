package fedapi

import (
	"strings"

	"maunium.net/go/mautrix/federation"
)

// originFromAuth extracts the claimed origin server from an X-Matrix
// Authorization header.
//
// Phase 1 does not verify the signature — Synapse does that downstream — so the
// value is for logging and metrics only and must not be used for any access
// decision. Native serving will replace this with federation.ServerAuth, which
// verifies before returning an origin.
func originFromAuth(header string) string {
	if !strings.HasPrefix(header, "X-Matrix ") {
		return ""
	}
	return federation.ParseXMatrixAuth(header).Origin
}
