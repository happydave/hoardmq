// Package topic implements MQTT topic name and topic filter rules: filter
// validation, wildcard matching and the $-topic exclusion, per MQTT 5.0
// section 4.7. The rules are the extension API's (ext/mqtt); these names are
// the core's.
package topic

import "github.com/happydave/hoardmq/ext/mqtt"

// SharePrefix introduces a shared subscription filter.
const SharePrefix = mqtt.SharePrefix

// ValidateName reports why a topic name is invalid, or "" when it is valid.
func ValidateName(name string) string { return mqtt.ValidateTopicName(name) }

// ValidateFilter reports why a topic filter is invalid, or "" when it is valid.
func ValidateFilter(filter string) string { return mqtt.ValidateTopicFilter(filter) }

// HasWildcard reports whether the filter contains '+' or '#'.
func HasWildcard(filter string) bool { return mqtt.HasWildcard(filter) }

// IsShared reports whether the filter is a shared subscription filter.
func IsShared(filter string) bool { return mqtt.IsSharedFilter(filter) }

// SplitShared returns the share name and the inner filter of a shared
// subscription filter, or "" and the filter itself when it is not shared.
func SplitShared(filter string) (share, inner string) { return mqtt.SplitSharedFilter(filter) }

// Match reports whether a valid filter matches a topic name.
func Match(filter, name string) bool { return mqtt.MatchTopic(filter, name) }
