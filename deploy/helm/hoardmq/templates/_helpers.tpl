{{/*
Chart name and fully qualified release name.
*/}}
{{- define "hoardmq.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "hoardmq.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "hoardmq.headless" -}}
{{- printf "%s-headless" (include "hoardmq.fullname" .) -}}
{{- end -}}

{{- define "hoardmq.labels" -}}
app.kubernetes.io/name: {{ include "hoardmq.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "hoardmq.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hoardmq.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
The Secret holding the cluster CA and node certificate.
*/}}
{{- define "hoardmq.tlsSecret" -}}
{{- printf "%s-cluster-tls" (include "hoardmq.fullname" .) -}}
{{- end -}}

{{/*
The Secret holding the admin API's tokens.
*/}}
{{- define "hoardmq.adminSecret" -}}
{{- printf "%s-admin" (include "hoardmq.fullname" .) -}}
{{- end -}}

{{/*
Termination grace: the drain's two timeouts plus a margin.
*/}}
{{- define "hoardmq.terminationGrace" -}}
{{- add .Values.cluster.drain.timeoutSeconds .Values.cluster.drain.confirmTimeoutSeconds .Values.cluster.drain.graceMarginSeconds -}}
{{- end -}}
