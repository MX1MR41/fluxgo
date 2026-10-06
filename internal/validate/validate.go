// Package validate centralizes validation of names that end up as paths on
// disk (topics, consumer group IDs). Keeping the rules in one place means
// the store and the offset manager cannot drift apart, and no name can ever
// escape the data directory.
package validate

import (
	"errors"
	"fmt"
)

// maxNameLen mirrors Kafka's limit for topic names.
const maxNameLen = 249

// ErrInvalidName is returned (wrapped) for any rejected name.
var ErrInvalidName = errors.New("invalid name")

func check(name, what string) error {
	if name == "" {
		return fmt.Errorf("%w: %s must not be empty", ErrInvalidName, what)
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("%w: %s longer than %d characters", ErrInvalidName, what, maxNameLen)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%w: %s %q is not allowed", ErrInvalidName, what, name)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '-', r == '_':
		default:
			return fmt.Errorf("%w: %s %q contains illegal character %q (allowed: a-z A-Z 0-9 . - _)",
				ErrInvalidName, what, name, r)
		}
	}
	return nil
}

// Topic validates a topic name.
func Topic(topic string) error { return check(topic, "topic name") }

// GroupID validates a consumer group ID.
func GroupID(group string) error { return check(group, "group id") }
