{{/*
Helpers shared by every template in this chart.

Files starting with an underscore are NOT rendered into Kubernetes objects —
Helm treats them as a library of named templates the other files include.
*/}}

{{/*
The chart's name, used as app.kubernetes.io/name.
*/}}
{{- define "chat-go.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
The prefix every object in this release is named with.

Normally "<release>-<chart>", but if the release name already contains the
chart name it is used as-is — so `helm install chat-go ./chat-go` produces
"chat-go-postgres" and not the stuttering "chat-go-chat-go-postgres".

Truncated at 63 because that is the maximum length of a DNS label, and these
names become DNS names via Services.
*/}}
{{- define "chat-go.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
SELECTOR LABELS — the identity a Service or a controller matches on.

Called with a dict, because a named template receives exactly one argument and
this one needs two:

    {{- include "chat-go.selectorLabels" (dict "root" $ "component" "postgres") }}

"root" is $ , the top-level scope, so .Chart and .Release stay reachable inside.

app.kubernetes.io/component is NOT decoration. Seven components share one
release, and without it every Service selector would match every pod in the
chart. That precise bug cost time on StudentSystemGo: the app's Service also
selected the MySQL pods, and port-forward failed with "no named port http".

These labels must NEVER change on a live release. A Deployment's selector is
immutable and a StatefulSet rejects the edit outright, so the only way out is
to delete and recreate the object. That is why the chart version and app
version are NOT in here — they change on every upgrade, and a selector cannot.
*/}}
{{- define "chat-go.selectorLabels" -}}
app.kubernetes.io/name: {{ include "chat-go.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
FULL LABELS — everything above, plus metadata that is safe to change.

Applied to an object's own metadata.labels, never to a selector. Same dict
argument as the selector labels.
*/}}
{{- define "chat-go.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "chat-go.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/part-of: chat-go
{{- end }}

{{/*
The Postgres connection string, assembled once.

Five processes need it — migrate, gateway, persister, indexer, bot — and a
typo in any one of them would show up as a single component that mysteriously
cannot reach the database while the rest are fine. Built here, that is one
place to be wrong instead of five.

The host is the headless Service's name, which resolves inside the cluster to
the Postgres pod. Port 5432, not the 5433 used locally: that offset exists only
to dodge news-feed-go on the laptop, and inside a cluster nothing collides.

sslmode=disable is honest for now — this traffic stays on the pod network — and
is the kind of thing a network policy should be enforcing rather than a
connection string requesting. Tracked with the rest of the blockers.

Note that this embeds the password, so it lands in a rendered manifest. That is
blocker B5 in docs/HARDENING.md and the very next thing we fix.
*/}}
{{- define "chat-go.databaseURL" -}}
postgres://{{ .Values.postgres.user }}:{{ .Values.postgres.password }}@{{ include "chat-go.fullname" . }}-postgres:5432/{{ .Values.postgres.database }}?sslmode=disable
{{- end }}
