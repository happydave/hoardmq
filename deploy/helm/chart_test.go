// Package helm holds the chart's self-check: without Helm on the build
// machine, it keeps the templates consistent with values.yaml and asserts
// the deployment properties the design requires are literally present.
package helm

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const chartDir = "hoardmq"

// valuesKeys returns every dotted key path in the chart's values.yaml. The
// file is this chart's own and uses two-space indented maps with scalar or
// map values; that is all the walker handles.
func valuesKeys(t *testing.T) map[string]bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	var stack []string
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "- ") {
			continue
		}
		indent := (len(line) - len(strings.TrimLeft(line, " "))) / 2
		name, _, _ := strings.Cut(trimmed, ":")
		if indent > len(stack) {
			t.Fatalf("values.yaml: unexpected indent at %q", line)
		}
		stack = append(stack[:indent], name)
		keys[strings.Join(stack, ".")] = true
	}
	return keys
}

// valuesScalars returns each dotted key path in values.yaml that holds a
// scalar, with its value as written.
func valuesScalars(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	var stack []string
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "- ") {
			continue
		}
		indent := (len(line) - len(strings.TrimLeft(line, " "))) / 2
		name, value, _ := strings.Cut(trimmed, ":")
		stack = append(stack[:min(indent, len(stack))], name)
		if value = strings.TrimSpace(value); value != "" {
			out[strings.Join(stack, ".")] = value
		}
	}
	return out
}

func templates(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(filepath.Join(chartDir, "templates"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(chartDir, "templates", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = string(data)
	}
	return out
}

func TestEveryReferencedValueExists(t *testing.T) {
	keys := valuesKeys(t)
	ref := regexp.MustCompile(`\.Values\.([A-Za-z0-9_.]+)`)
	var missing []string
	for name, body := range templates(t) {
		for _, m := range ref.FindAllStringSubmatch(body, -1) {
			path := strings.TrimRight(m[1], ".")
			if !keys[path] {
				missing = append(missing, name+": "+path)
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("templates reference values not in values.yaml:\n%s", strings.Join(missing, "\n"))
	}
}

func TestActionsAreBalanced(t *testing.T) {
	for name, body := range templates(t) {
		if strings.Count(body, "{{") != strings.Count(body, "}}") {
			t.Errorf("%s: unbalanced template actions", name)
		}
	}
}

func TestChartMetadata(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(chartDir, "Chart.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"apiVersion: v2", "name: hoardmq", "version: ", "appVersion: "} {
		if !strings.Contains(string(data), want) {
			t.Errorf("Chart.yaml lacks %q", want)
		}
	}
	if _, err := os.Stat(filepath.Join(chartDir, "README.md")); err != nil {
		t.Error("chart README missing")
	}
}

// TestDesignedDeploymentProperties: the StatefulSet and Services carry the
// properties design invariant 10 and the Kubernetes deployment section
// require, as literal text.
func TestDesignedDeploymentProperties(t *testing.T) {
	tpl := templates(t)
	checks := []struct{ file, want string }{
		{"statefulset.yaml", "podManagementPolicy: Parallel"},
		// An upgrade replaces one pod at a time, each drained by its preStop.
		{"statefulset.yaml", "updateStrategy:\n    type: RollingUpdate"},
		{"statefulset.yaml", "terminationGracePeriodSeconds:"},
		{"statefulset.yaml", "preStop:"},
		{"statefulset.yaml", "- drain"},
		{"statefulset.yaml", "- -wait"},
		{"statefulset.yaml", "path: /readyz"},
		{"statefulset.yaml", "path: /healthz"},
		// The liveness probe restarts a pod about 30 s after /healthz starts
		// failing.
		{"statefulset.yaml", "path: /healthz\n              port: observability\n            periodSeconds: 10\n            timeoutSeconds: 2\n            failureThreshold: 3"},
		{"statefulset.yaml", "volumeClaimTemplates:"},
		{"statefulset.yaml", "HOARDMQ_NODE_NAME"},
		{"statefulset.yaml", "HOARDMQ_CLUSTER_ADVERTISE"},
		// Each pod presents the certificate whose common name is its own node
		// name; a shared one cannot authenticate a cluster.
		{"statefulset.yaml", "subPathExpr: $(HOARDMQ_NODE_NAME).crt"},
		// Kubernetes' Service links would land in the broker's HOARDMQ_
		// environment-override namespace and be rejected as unknown.
		{"statefulset.yaml", "enableServiceLinks: false"},
		{"statefulset.yaml", "runAsNonRoot: true"},
		{"service-headless.yaml", "publishNotReadyAddresses: true"},
		{"service-headless.yaml", "clusterIP: None"},
		{"pdb.yaml", "maxUnavailable: 1"},
		{"configmap.yaml", `"mode" "mtls"`},
		{"configmap.yaml", `"mode" "dns"`},
		// The probes, /metrics and /drain are served by this module; without
		// it the observability address binds nothing and no pod turns ready.
		{"configmap.yaml", `"metrics.prometheus"`},
		{"secret-tls.yaml", "genCA"},
		{"secret-tls.yaml", "lookup"},
		{"secret-tls.yaml", "helm.sh/resource-policy: keep"},
		{"certificate.yaml", "kind: Certificate"},
	}
	for _, c := range checks {
		if !strings.Contains(tpl[c.file], c.want) {
			t.Errorf("%s lacks %q", c.file, c.want)
		}
	}
	values, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(values), "mode: generated") {
		t.Error("mutual TLS is not the default")
	}
}

// TestReadinessProbeStaysOnReadyz (WI 1571): the readiness probe reads
// /readyz, and /servingz is no probe, so a degraded node stays in service.
func TestReadinessProbeStaysOnReadyz(t *testing.T) {
	sts := templates(t)["statefulset.yaml"]
	probe := regexp.MustCompile(`(?s)readinessProbe:\s*httpGet:\s*path:\s*(\S+)`).FindStringSubmatch(sts)
	if probe == nil || probe[1] != "/readyz" {
		t.Fatalf("the readiness probe's path: %v", probe)
	}
	for name, text := range templates(t) {
		if strings.Contains(text, "/servingz") {
			t.Errorf("%s names /servingz", name)
		}
	}
}

// TestTerminationGraceHoldsTheWholeDrain: a drain waits for its redirected
// clients (timeoutSeconds) and then for its handed-off sessions to be
// confirmed (confirmTimeoutSeconds), so the grace the kubelet gives the
// preStop hook and the process is both plus the margin. A grace short of
// that kills the pod while its drain waits on the only copy of a session.
func TestTerminationGraceHoldsTheWholeDrain(t *testing.T) {
	tpl := templates(t)
	helper := regexp.MustCompile(`(?s)define "hoardmq.terminationGrace".*?end`).FindString(tpl["_helpers.tpl"])
	for _, v := range []string{".Values.cluster.drain.timeoutSeconds", ".Values.cluster.drain.confirmTimeoutSeconds", ".Values.cluster.drain.graceMarginSeconds"} {
		if !strings.Contains(helper, v) {
			t.Errorf("hoardmq.terminationGrace does not add %s: %q", v, helper)
		}
	}
	sts := tpl["statefulset.yaml"]
	if !strings.Contains(sts, `terminationGracePeriodSeconds: {{ include "hoardmq.terminationGrace" . }}`) {
		t.Error("terminationGracePeriodSeconds is not the drain's grace")
	}
	if !strings.Contains(sts, `- {{ include "hoardmq.terminationGrace" . }}s`) {
		t.Error("the preStop drain's -timeout is not the drain's grace")
	}
	if !strings.Contains(tpl["configmap.yaml"], `"confirmTimeoutMs" (mul 1000 (int .Values.cluster.drain.confirmTimeoutSeconds))`) {
		t.Error("the configuration does not carry the chart's confirmation timeout")
	}
}

// TestBrokerHoldsItsExitForThePreStop: the broker exits after its drain only
// once the hook has returned (the SIGTERM) or the hold ends, so the hook is
// not killed with it; a hold of 0 brings the kill back.
func TestBrokerHoldsItsExitForThePreStop(t *testing.T) {
	if !strings.Contains(templates(t)["configmap.yaml"], `"holdExitMs" (mul 1000 (int .Values.cluster.drain.holdExitSeconds))`) {
		t.Error("the configuration does not carry the chart's exit hold")
	}
	values, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^    holdExitSeconds: [1-9][0-9]*$`).Match(values) {
		t.Error("values.yaml does not set cluster.drain.holdExitSeconds to a positive number")
	}
}

// TestPreStopDrainsThroughTheAdminAPIWhenEnabled: with the admin API on, the
// observability listener's /drain is closed, so the preStop must drain
// through the API with the chart-generated drain token; with it off the
// preStop is unchanged. Both branches wait and carry the drain's grace, and
// the token file the preStop reads is the one the Secret, the mount and the
// configuration name.
func TestPreStopDrainsThroughTheAdminAPIWhenEnabled(t *testing.T) {
	tpl := templates(t)
	sts := tpl["statefulset.yaml"]
	preStop := regexp.MustCompile(`(?s)preStop:.*?resources:`).FindString(sts)
	branches := regexp.MustCompile(`(?s)- drain\s*\{\{- if \.Values\.admin\.enabled \}\}(.*?)\{\{- else \}\}(.*?)\{\{- end \}\}\s*- -wait\s*- -timeout\s*- \{\{ include "hoardmq\.terminationGrace" \. \}\}s`).FindStringSubmatch(preStop)
	if branches == nil {
		t.Fatalf("the preStop does not branch on admin.enabled before -wait and the grace:\n%s", preStop)
	}
	adminBranch, defaultBranch := branches[1], branches[2]
	for _, want := range []string{"- -admin", "- 127.0.0.1:{{ .Values.admin.port }}", "- -token-file", "- /etc/hoardmq/admin/drain.token"} {
		if !strings.Contains(adminBranch, want) {
			t.Errorf("the admin branch lacks %q", want)
		}
	}
	for _, want := range []string{"- -address", "- 127.0.0.1:{{ .Values.observability.port }}"} {
		if !strings.Contains(defaultBranch, want) {
			t.Errorf("the default branch lacks %q", want)
		}
	}
	if !strings.Contains(sts, "mountPath: /etc/hoardmq/admin") || !strings.Contains(sts, `secretName: {{ include "hoardmq.adminSecret" . }}`) {
		t.Error("the admin Secret is not mounted at /etc/hoardmq/admin")
	}
	secret := tpl["secret-admin.yaml"]
	for _, want := range []string{"{{- if .Values.admin.enabled }}", "lookup", "helm.sh/resource-policy: keep", "drain.token: {{ randAlphaNum 48 | b64enc }}", "operator.token:"} {
		if !strings.Contains(secret, want) {
			t.Errorf("secret-admin.yaml lacks %q", want)
		}
	}
	cm := tpl["configmap.yaml"]
	for _, want := range []string{`"tokenFile" "/etc/hoardmq/admin/drain.token" "scopes" (list "cluster:drain" "cluster:read")`, `(printf "127.0.0.1:%d" (int .Values.admin.port))`, "{{- if .Values.admin.enabled }}"} {
		if !strings.Contains(cm, want) {
			t.Errorf("configmap.yaml lacks %q", want)
		}
	}
	// The admin API configured through extraConfig alone would close the
	// /drain the default preStop posts to; the chart refuses to render it.
	if !regexp.MustCompile(`if and \(not \.Values\.admin\.enabled\) \(hasKey \.Values\.extraConfig "admin"\) \}\}\s*\{\{- fail `).MatchString(cm) {
		t.Error("configmap.yaml does not refuse extraConfig.admin without admin.enabled")
	}
	values, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?m)^admin:\n(?:  #.*\n)*  enabled: false$`).Match(values) {
		t.Error("the admin API is not off by default in values.yaml")
	}
}

// TestObservabilityDrainIsLoopbackOnly: the observability listener binds all
// interfaces for the probes and scrapes, and its /drain answers only loopback
// callers, so the default preStop must reach it on the pod's loopback and the
// README must not present the listener's drain as reachable from the network.
func TestObservabilityDrainIsLoopbackOnly(t *testing.T) {
	tpl := templates(t)
	preStop := regexp.MustCompile(`(?s)preStop:.*?resources:`).FindString(tpl["statefulset.yaml"])
	addr := regexp.MustCompile(`(?s)\{\{- else \}\}\s*- -address\s*- (\S+):\{\{ \.Values\.observability\.port \}\}`).FindStringSubmatch(preStop)
	if addr == nil || addr[1] != "127.0.0.1" {
		t.Errorf("the default preStop does not drain over the loopback: %v", addr)
	}
	if !strings.Contains(tpl["configmap.yaml"], `"observability" (dict "address" (printf "0.0.0.0:%d" (int .Values.observability.port)) "allowRemote" true)`) {
		t.Error("the observability listener no longer binds all interfaces for the probes and /metrics")
	}
	data, err := os.ReadFile(filepath.Join(chartDir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	readme := strings.Join(strings.Fields(string(data)), " ")
	if !strings.Contains(readme, "Its `/drain` answers only loopback callers") {
		t.Error("the chart README does not say /drain answers only loopback callers")
	}
	if strings.Contains(readme, "Restrict it with a NetworkPolicy") {
		t.Error("the chart README still offers a NetworkPolicy as the protection for /drain")
	}
}

// TestChartSetsTheLimits: the broker's own connect-rate, connection and
// subscription limits are unlimited by default, so the chart sets each to a
// positive number, carries it into the configuration document, and says in
// its README why each value is what it is.
func TestChartSetsTheLimits(t *testing.T) {
	values := valuesScalars(t)
	for _, key := range []string{"limits.connectRate", "limits.maxSubscriptions", "listeners.mqtt.maxConnections", "listeners.mqtts.maxConnections"} {
		if n, err := strconv.Atoi(values[key]); err != nil || n <= 0 {
			t.Errorf("values.yaml sets %s to %q, want a positive number", key, values[key])
		}
	}
	cm := templates(t)["configmap.yaml"]
	for _, want := range []string{
		`"limits" (dict "connectRate" (int .Values.limits.connectRate) "connectBurst" (int .Values.limits.connectBurst) "maxSubscriptions" (int .Values.limits.maxSubscriptions))`,
		`"maxConnections" (int .Values.listeners.mqtt.maxConnections)`,
		`"maxConnections" (int .Values.listeners.mqtts.maxConnections)`,
	} {
		if !strings.Contains(cm, want) {
			t.Errorf("configmap.yaml lacks %q", want)
		}
	}
	data, err := os.ReadFile(filepath.Join(chartDir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	readme := string(data)
	for _, want := range []string{"## Limits", "`limits.connectRate`", "`limits.maxSubscriptions`", "`listeners.mqtt.maxConnections`"} {
		if !strings.Contains(readme, want) {
			t.Errorf("the chart README lacks %q", want)
		}
	}
}

// TestClusterTLSPerPodAndScalable: the broker container mounts only its own
// certificate, key and the CA, in both modes; the CA's key lives in a Secret
// no pod mounts, so a later upgrade can sign a new ordinal's certificate and
// fails, naming the Secret, when none is stored; cert-manager issues from one
// CA, with both auth usages a peer requires.
func TestClusterTLSPerPodAndScalable(t *testing.T) {
	tpl := templates(t)
	sts := tpl["statefulset.yaml"]
	mounts := regexp.MustCompile(`(?m)^ +- name: cluster-tls\n +mountPath: (\S+)\n +(subPath|subPathExpr): (\S+)$`).FindAllStringSubmatch(sts, -1)
	want := map[string]string{
		"/etc/hoardmq-tls/tls.crt": "$(HOARDMQ_NODE_NAME).crt",
		"/etc/hoardmq-tls/tls.key": "$(HOARDMQ_NODE_NAME).key",
		"/etc/hoardmq-tls/ca.crt":  "ca.crt",
		"/etc/hoardmq-tls":         "$(HOARDMQ_NODE_NAME)",
	}
	if len(mounts) != len(want) {
		t.Fatalf("cluster-tls mounts %q, want one per path of %v", mounts, want)
	}
	for _, m := range mounts {
		if want[m[1]] != m[3] {
			t.Errorf("cluster-tls at %s selects %q, want %q", m[1], m[3], want[m[1]])
		}
	}
	if n := len(regexp.MustCompile(`- name: cluster-tls\n +mountPath:`).FindAllString(sts, -1)); n != len(want) {
		t.Errorf("cluster-tls is mounted %d times, %d of them selecting the pod's files", n, len(want))
	}
	if strings.Contains(sts, "tlsCASecret") || strings.Contains(sts, "cluster-ca") {
		t.Error("the StatefulSet references the CA Secret")
	}
	for _, f := range []string{`"certFile" "/etc/hoardmq-tls/tls.crt"`, `"keyFile" "/etc/hoardmq-tls/tls.key"`, `"caFile" "/etc/hoardmq-tls/ca.crt"`} {
		if !strings.Contains(tpl["configmap.yaml"], f) {
			t.Errorf("configmap.yaml lacks %s", f)
		}
	}
	secret := tpl["secret-tls.yaml"]
	for _, want := range []string{
		`lookup "v1" "Secret" .Release.Namespace $caName`,
		`buildCustomCert (index $existingCA.data "ca.crt") (index $existingCA.data "ca.key")`,
		`if not (hasKey $data (printf "%s.crt" $pod))`,
		`has no certificate for %s and no CA key is stored to sign one: delete Secret %s and upgrade`,
		`holds certificates from another CA than Secret %s`,
	} {
		if !strings.Contains(secret, want) {
			t.Errorf("secret-tls.yaml lacks %q", want)
		}
	}
	nodeSecret, caSecret, ok := strings.Cut(secret, "---")
	if !ok || strings.Contains(nodeSecret, "ca.key:") || !strings.Contains(caSecret, "ca.key:") || !strings.Contains(caSecret, "name: {{ $caName }}") {
		t.Error("the CA key is not in the CA Secret alone")
	}
	cert := tpl["certificate.yaml"]
	for _, want := range []string{"selfSigned: {}", "isCA: true", "ca:\n    secretName: {{ include \"hoardmq.tlsCASecret\" . }}", "    - server auth\n    - client auth"} {
		if !strings.Contains(cert, want) {
			t.Errorf("certificate.yaml lacks %q", want)
		}
	}
	data, err := os.ReadFile(filepath.Join(chartDir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	readme := strings.Join(strings.Fields(string(data)), " ")
	for _, want := range []string{"subPathExpr", "csi-driver", "<release>-cluster-ca"} {
		if !strings.Contains(readme, want) {
			t.Errorf("the chart README lacks %q", want)
		}
	}
}

// TestScaleWithinTheMaximumRestartsNothing: the peer list comes from
// cluster.maxReplicas, so changing replicaCount up to it changes neither the
// configuration document nor the pod template, and the StatefulSet replaces
// no running pod (a rolling update would start the new pod first, which the
// running pods, on the old list, would never dial). Only the pod count and
// the generated node Secret follow replicaCount; cert-manager's Certificates
// cover every ordinal below the maximum, and a replicaCount above it is
// refused at render.
func TestScaleWithinTheMaximumRestartsNothing(t *testing.T) {
	tpl := templates(t)
	if n, err := strconv.Atoi(valuesScalars(t)["cluster.maxReplicas"]); err != nil || n < 3 {
		t.Fatalf("values.yaml sets cluster.maxReplicas to %q, want at least the default replicaCount", valuesScalars(t)["cluster.maxReplicas"])
	}
	if !strings.Contains(tpl["configmap.yaml"], `"replicas" (int .Values.cluster.maxReplicas) "port"`) {
		t.Error("the configuration's discovery count is not cluster.maxReplicas")
	}
	allowed := map[string][]string{
		"statefulset.yaml": {"replicas: {{ .Values.replicaCount }}"},
		"secret-tls.yaml":  {"{{- range $i := until (int .Values.replicaCount) }}"},
		"NOTES.txt":        {"with {{ .Values.replicaCount }} pods"},
		"configmap.yaml":   {"if gt (int .Values.replicaCount) (int .Values.cluster.maxReplicas)", "(int .Values.replicaCount) (int .Values.cluster.maxReplicas)) }}"},
	}
	for name, body := range tpl {
		for _, line := range strings.Split(body, "\n") {
			if !strings.Contains(line, ".Values.replicaCount") {
				continue
			}
			ok := false
			for _, a := range allowed[name] {
				ok = ok || strings.Contains(line, a)
			}
			if !ok {
				t.Errorf("%s reads replicaCount outside the pod count, the generated Secret and the refusal: %s", name, strings.TrimSpace(line))
			}
		}
	}
	sts := tpl["statefulset.yaml"]
	if !regexp.MustCompile(`range \$i := until \(int \.Values\.cluster\.maxReplicas\) \}\}\n.*\n +- secret:\n +name: [^\n]+\n +items:`).MatchString(sts) {
		t.Error("certManager's projected volume does not list every ordinal below the maximum")
	}
	// An optional source lets a pod start before its own Secret is issued,
	// on an empty directory the Secret never fills.
	if strings.Contains(sts, "optional: true") {
		t.Error("certManager's projected sources are optional")
	}
	if !strings.Contains(tpl["certificate.yaml"], "{{- range $i := until (int .Values.cluster.maxReplicas) }}") {
		t.Error("certManager's Certificates do not cover every ordinal below the maximum")
	}
	if !regexp.MustCompile(`if gt \(int \.Values\.replicaCount\) \(int \.Values\.cluster\.maxReplicas\) \}\}\n\{\{- fail `).MatchString(tpl["configmap.yaml"]) {
		t.Error("configmap.yaml does not refuse a replicaCount above cluster.maxReplicas")
	}
	data, err := os.ReadFile(filepath.Join(chartDir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	readme := strings.Join(strings.Fields(string(data)), " ")
	for _, want := range []string{"## Scaling", "`cluster.maxReplicas`", "first raise `cluster.maxReplicas` alone", "restarts nothing", "30 s by default"} {
		if !strings.Contains(readme, want) {
			t.Errorf("the chart README lacks %q", want)
		}
	}
	if strings.Contains(readme, "needs a hand step") {
		t.Error("the chart README still asks for a hand step on scale-up")
	}
}
