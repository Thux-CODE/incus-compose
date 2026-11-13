package dns

import (
	"encoding/json"
	"strings"

	incusutil "github.com/lxc/incus/v7/shared/util"

	"github.com/lxc/incus-compose/ievent/iutil"
	"github.com/lxc/incus-compose/shared"
)

// ready reports whether a replica may answer for its service while another
// may not. One ic-healthd checks is ready once a check has passed, and not
// before: whatever else is on it - nothing yet, stopped from before its start,
// starting, unhealthy - says it has not passed one since it last started. One
// nothing checks is ready as it always was.
//
// It decides between replicas and never takes a service down: services answers
// with every replica where none is ready.
func ready(inst *iutil.Instance) bool {
	if !watched(inst) {
		return true
	}

	status, _ := inst.ConfigValue(shared.HealthStatusKey)

	return strings.TrimSpace(status) == shared.HealthStatusHealthy
}

// watched reports whether ic-healthd runs a check on this instance: enabled,
// with a test, and a test that is not compose's NONE.
func watched(inst *iutil.Instance) bool {
	enabled, _ := inst.ConfigValue(shared.HealthEnabledKey)
	if !incusutil.IsTrue(enabled) {
		return false
	}

	raw, _ := inst.ConfigValue(shared.HealthKeyPrefix + "test")

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}

	var test []string

	err := json.Unmarshal([]byte(raw), &test)
	if err != nil {
		// Not a list compose wrote, but a test all the same.
		return true
	}

	return len(test) > 0 && test[0] != "NONE"
}
