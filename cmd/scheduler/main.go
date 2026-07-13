// Command scheduler is the job-placement service. Three instances run in
// compose to form the future Raft quorum, but Phase 0 deliberately wires no
// election — each instance is a health stub until Phase 3.
package main

import "github.com/sohamghodake/ironwork/internal/app"

func main() {
	app.RunStub("scheduler")
}
