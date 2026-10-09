// Package topic implements MQTT topic name and topic filter rules: filter
// validation, wildcard matching and the $-topic exclusion, per MQTT 5.0
// section 4.7.
package topic

import "strings"

// SharePrefix introduces a shared subscription filter.
const SharePrefix = "$share/"

// ValidateName reports why a topic name is invalid, or "" when it is valid:
// non-empty, no wildcard characters.
func ValidateName(name string) string {
	if name == "" {
		return "topic name is empty"
	}
	if strings.ContainsAny(name, "+#") {
		return "topic name contains a wildcard character"
	}
	return ""
}

// ValidateFilter reports why a topic filter is invalid, or "" when it is
// valid: non-empty; '#' only as the whole last level; '+' only as a whole
// level. A shared subscription filter is validated as its inner filter with
// a non-empty share name free of wildcards and slashes.
func ValidateFilter(filter string) string {
	if filter == "" {
		return "topic filter is empty"
	}
	if strings.HasPrefix(filter, SharePrefix) {
		rest := filter[len(SharePrefix):]
		name, inner, ok := strings.Cut(rest, "/")
		if !ok {
			return "shared subscription filter has no topic filter after the share name"
		}
		if name == "" {
			return "shared subscription share name is empty"
		}
		if strings.ContainsAny(name, "+#") {
			return "shared subscription share name contains a wildcard character"
		}
		return ValidateFilter(inner)
	}
	levels := strings.Split(filter, "/")
	for i, l := range levels {
		switch {
		case l == "#":
			if i != len(levels)-1 {
				return "'#' must be the last level of the topic filter"
			}
		case l == "+":
		case strings.ContainsAny(l, "+#"):
			return "wildcard characters must occupy a whole level"
		}
	}
	return ""
}

// HasWildcard reports whether the filter contains '+' or '#'.
func HasWildcard(filter string) bool { return strings.ContainsAny(filter, "+#") }

// IsShared reports whether the filter is a shared subscription filter.
func IsShared(filter string) bool { return strings.HasPrefix(filter, SharePrefix) }

// SplitShared returns the share name and the inner filter of a shared
// subscription filter, or "" and the filter itself when it is not shared.
func SplitShared(filter string) (share, inner string) {
	if !IsShared(filter) {
		return "", filter
	}
	share, inner, _ = strings.Cut(filter[len(SharePrefix):], "/")
	return share, inner
}

// Match reports whether a valid filter matches a topic name. A filter that
// starts with a wildcard does not match a name that starts with '$'.
func Match(filter, name string) bool {
	if name == "" {
		return false
	}
	if name[0] == '$' && (filter == "" || filter[0] == '+' || filter[0] == '#') {
		return false
	}
	f, n := filter, name
	for {
		fl, frest, fmore := strings.Cut(f, "/")
		if fl == "#" {
			return true
		}
		nl, nrest, nmore := strings.Cut(n, "/")
		if fl != "+" && fl != nl {
			return false
		}
		if !fmore && !nmore {
			return true
		}
		if fmore != nmore {
			// "a/#" against "a" is handled by the '#' rule on the next round.
			if fmore && frest == "#" {
				return true
			}
			return false
		}
		f, n = frest, nrest
	}
}
