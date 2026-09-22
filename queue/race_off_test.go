//go:build !race

package queue

// raceEnabled reports whether the race detector is on. The detector slows the
// scheduler enough to change the timing of the concurrency tests, which are
// built around narrow interleavings rather than around throughput.
const raceEnabled = false
