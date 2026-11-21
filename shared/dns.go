package shared

import "strings"

// DNSDrained reports whether a value of DNSDrainKey drains the instance, as
// ic-dns reads it: the label's value trimmed, then true, 1, yes or on in any
// case.
func DNSDrained(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "1", "yes", "on":
		return true
	default:
		return false
	}
}

// DNS constants for ic-dns configuration and discovery.
const (
	DNSKeyPrefix = "user.dns."

	// DNSDaemonScopeKey is where the daemon instance stores its scope.
	DNSDaemonScopeKey = DNSKeyPrefix + "scope"

	// DNSScopeKey is the project config key opting a project into DNS.
	DNSScopeKey = "user.label.dns.scope"

	// DNSZoneKey is the project config key setting the DNS zone.
	DNSZoneKey = "user.label.dns.zone"

	// DNSTransferKey is the project config key opting a project into zone transfers.
	DNSTransferKey = "user.label.dns.transfer"

	// DNSDrainKey when "true" on an instance takes it out of its service name
	// while it keeps answering at its own.
	DNSDrainKey = "user.label.dns.drain"

	DNSScopeProject = "project"
	DNSScopeGlobal  = "global"
)
