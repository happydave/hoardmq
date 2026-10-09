package sim

import (
	"math"
	"reflect"
	"testing"

	"github.com/happydave/hoardmq/internal/module"
)

func TestPickStormMetrics(t *testing.T) {
	got := pickStormMetrics([]module.Metric{
		{Name: "hoardmq_lease_proposals_total", Labels: map[string]string{"op": "acquire"}, Value: 3},
		{Name: "hoardmq_lease_proposals_total", Labels: map[string]string{"op": "alive"}, Value: 2},
		{Name: "hoardmq_lease_declarations_total", Value: 1},
		{Name: "hoardmq_route_owners_hidden", Labels: map[string]string{"reason": "adopting", "b": "x"}, Value: 4},
		{Name: "hoardmq_sessions_active", Labels: map[string]string{"listener": "tcp"}, Value: 9},
		{Name: "hoardmq_lease_nodes_declared_dead", Value: math.NaN()},
		{Name: "hoardmq_replication_refusal_fallback_total", Labels: map[string]string{"peer": "n3", "kind": "no-key"}, Value: 5},
	})
	want := map[string]float64{
		`hoardmq_lease_proposals_total{op="acquire"}`:                         3,
		`hoardmq_lease_proposals_total{op="alive"}`:                           2,
		`hoardmq_lease_nodes_declared_dead`:                                   -1,
		`hoardmq_lease_declarations_total`:                                    1,
		`hoardmq_route_owners_hidden{b="x",reason="adopting"}`:                4,
		`hoardmq_replication_refusal_fallback_total{kind="no-key",peer="n3"}`: 5,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// A run records what the password cache did and what the connect-rate limits
// refused, beside the rest of the storm (WI 1283).
func TestStormMetricsCarryTheConnectPathBudget(t *testing.T) {
	got := pickStormMetrics([]module.Metric{
		{Name: "hoardmq_auth_password_derivations_total", Value: 40},
		{Name: "hoardmq_auth_password_cache_hits_total", Value: 200},
		{Name: "hoardmq_auth_password_cache_entries", Value: 40},
		{Name: "hoardmq_connect_rate_refused_total", Labels: map[string]string{"listener": "tcp", "limit": "node"}, Value: 7},
	})
	want := map[string]float64{
		`hoardmq_auth_password_derivations_total`:                         40,
		`hoardmq_auth_password_cache_hits_total`:                          200,
		`hoardmq_auth_password_cache_entries`:                             40,
		`hoardmq_connect_rate_refused_total{limit="node",listener="tcp"}`: 7,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// A run records the overload tier's series (WI 1314).
func TestStormMetricsCarryTheOverloadTier(t *testing.T) {
	got := pickStormMetrics([]module.Metric{
		{Name: "hoardmq_overload_level", Labels: map[string]string{"scope": "cluster"}, Value: 0},
		{Name: "hoardmq_flapping_trips_total", Value: 0},
		{Name: "hoardmq_connections_ended_total", Labels: map[string]string{"how": "server"}, Value: 12},
		{Name: "hoardmq_deliveries_deferred_total", Labels: map[string]string{"listener": "tcp"}, Value: 3},
	})
	want := map[string]float64{
		`hoardmq_overload_level{scope="cluster"}`:           0,
		`hoardmq_flapping_trips_total`:                      0,
		`hoardmq_connections_ended_total{how="server"}`:     12,
		`hoardmq_deliveries_deferred_total{listener="tcp"}`: 3,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
