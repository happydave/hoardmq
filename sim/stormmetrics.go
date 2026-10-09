package sim

import (
	"math"
	"sort"
	"strings"

	"github.com/happydave/hoardmq/internal/module"
)

// stormMetricFamilies are the metrics a failover run records from each
// survivor around the storm: what the lease service proposed, by kind; the
// owners whose routes the routing plane hides, by reason; the leader's
// declarations; replica fetches by outcome; and what became of each CONNECT.
var stormMetricFamilies = map[string]bool{
	"hoardmq_lease_proposals_total":     true,
	"hoardmq_route_owners_hidden":       true,
	"hoardmq_lease_declarations_total":  true,
	"hoardmq_lease_nodes_declared_dead": true,
	"hoardmq_replication_fetches_total": true,
	// What became of each CONNECT: the lease acquire by outcome, the refusals
	// a node answered for the cluster's sake, and every refused CONNACK by
	// reason.
	"hoardmq_lease_ops_total":        true,
	"hoardmq_connect_refusals_total": true,
	"hoardmq_auth_failures_total":    true,
	// Why a forward to a peer did not complete, by reason. The labels
	// separate a frame that never left (`send`), a link that dropped with
	// frames in flight (`link-reset`) and a hole the receiver reported
	// (`sequence-gap`) — which is what tells a lost frame from a reordered
	// one without reading a node log (WI 1706).
	"hoardmq_forward_failures_total":             true,
	"hoardmq_replication_barrier_failures_total": true,
	// A forwarded frame that arrived below the sequence expected of it: the
	// frame a gap already reported missing, arriving late. It is what tells a
	// phantom hole from a lost one (WI 1708).
	"hoardmq_forward_late_frames_total": true,
	// A frame below the expectation that no reported hole accounts for: the
	// sender started a new stream. And every forward sent, by kind, which
	// gives the gap and late counts their denominator (WI 1708).
	"hoardmq_forward_stream_restarts_total": true,
	"hoardmq_forwards_total":                true,
	// A dispatch owner's relay of a group give's outcome to the publisher's
	// node, by outcome (WI 1896): what the probe's groups exercise (WI 1962).
	"hoardmq_group_gives_relayed_total": true,
	// A lost owner's routes forgotten because its sessions are served
	// elsewhere: WI 1625's end to the refusals a hidden owner causes, dated
	// against the recovery instant by the snapshot taken at it (WI 1734).
	"hoardmq_route_owners_forgotten_total": true,
	// A replication refusal the sender could not scope, by why: each one
	// blocks the stream to that peer whole until a resync (WI 1802).
	"hoardmq_replication_refusal_fallback_total": true,
	// A move a new owner's confirmation resolved, and the publishes a move
	// would have refused that were accepted on its proof, by path (WI 1806).
	"hoardmq_moves_resolved_total":       true,
	"hoardmq_move_refusals_lifted_total": true,
	// This node's served fact by how its proposal ended, and the gaps it found
	// in a peer's route updates, each of which holds a forget back until that
	// peer's next full set (WI 1633).
	"hoardmq_route_served_proposals_total": true,
	"hoardmq_route_update_gaps_total":      true,
	// With a JWT-authenticated population: the key-set fetches and the held
	// decisions made again.
	"hoardmq_auth_jwt_key_fetches_total": true,
	"hoardmq_auth_rechecks_total":        true,
	// The password verifications that derived a key and those the cache
	// answered, and the CONNECTs a connect-rate limit refused (WI 1283).
	"hoardmq_auth_password_derivations_total": true,
	"hoardmq_auth_password_cache_hits_total":  true,
	"hoardmq_auth_password_cache_entries":     true,
	"hoardmq_connect_rate_refused_total":      true,
	// The overload tier (WI 1314): the stress level, node and cluster, and its
	// indicators; what it refused and paused; the ban list's size, what it
	// refused and the detector's trips and bans; how connections ended; the
	// deliveries a full outbound queue deferred; the clients throttled and
	// for how long; the samples the controller took.
	"hoardmq_overload_level":                true,
	"hoardmq_overload_indicator":            true,
	"hoardmq_overload_refused_total":        true,
	"hoardmq_overload_shed_connections":     true,
	"hoardmq_bans":                          true,
	"hoardmq_connects_banned_total":         true,
	"hoardmq_flapping_trips_total":          true,
	"hoardmq_flapping_bans_total":           true,
	"hoardmq_connections_ended_total":       true,
	"hoardmq_deliveries_deferred_total":     true,
	"hoardmq_client_throttled_total":        true,
	"hoardmq_client_throttle_seconds_total": true,
	"hoardmq_overload_samples_total":        true,
	// With an LDAP-authenticated population: the directory's decisions, its
	// outages by policy, and the connections dialled to it.
	"hoardmq_auth_ldap_decisions_total":   true,
	"hoardmq_auth_ldap_unavailable_total": true,
	"hoardmq_auth_ldap_connections_total": true,
}

// pickStormMetrics keeps the storm families of a snapshot, keyed
// name{label="value",...} with the labels in name order. A value JSON cannot
// carry is recorded as -1, as sanitizeNaN does: one bad gauge must not cost
// the run its artifact.
func pickStormMetrics(snapshot []module.Metric) map[string]float64 {
	out := map[string]float64{}
	for _, m := range snapshot {
		if !stormMetricFamilies[m.Name] {
			continue
		}
		if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
			out[metricKey(m)] = -1
			continue
		}
		out[metricKey(m)] += m.Value
	}
	return out
}

func metricKey(m module.Metric) string {
	if len(m.Labels) == 0 {
		return m.Name
	}
	names := make([]string, 0, len(m.Labels))
	for k := range m.Labels {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, k := range names {
		parts = append(parts, k+`="`+m.Labels[k]+`"`)
	}
	return m.Name + "{" + strings.Join(parts, ",") + "}"
}
