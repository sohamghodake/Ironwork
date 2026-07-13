// Package logging builds the structured JSON logger shared by all components.
package logging

import (
	"os"

	"github.com/rs/zerolog"
)

// New returns a zerolog JSON logger tagged with the component kind and
// instance name. An unparseable level falls back to info.
func New(component, instance, level string) zerolog.Logger {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil || lvl == zerolog.NoLevel {
		lvl = zerolog.InfoLevel
	}
	return zerolog.New(os.Stdout).Level(lvl).With().
		Timestamp().
		Str("component", component).
		Str("instance", instance).
		Logger()
}
