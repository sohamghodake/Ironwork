// Command statemanager will own CRDT-replicated cluster state with visible
// divergence/reconvergence (Phase 5). Phase 0 runs it as a health stub.
package main

import "github.com/sohamghodake/ironwork/internal/app"

func main() {
	app.RunStub("statemanager")
}
