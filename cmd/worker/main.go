// Command worker executes placed jobs. Phase 0 runs it as a health stub;
// direct gateway->worker execution lands in Phase 1, backpressure and crash
// detection in Phase 4.
package main

import "github.com/sohamghodake/ironwork/internal/app"

func main() {
	app.RunStub("worker")
}
