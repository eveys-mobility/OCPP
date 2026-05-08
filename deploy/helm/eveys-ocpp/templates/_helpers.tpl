{{/*
Helpers — name + labels. Stock Helm boilerplate plus the gateway-
specific name shortcuts the templates lean on.
*/}}

{{- define "eveys-ocpp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "eveys-ocpp.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "eveys-ocpp.gatewayName" -}}
{{ include "eveys-ocpp.fullname" . }}-gateway
{{- end -}}

{{- define "eveys-ocpp.envoyName" -}}
{{ include "eveys-ocpp.fullname" . }}-envoy
{{- end -}}

{{- define "eveys-ocpp.commonLabels" -}}
app.kubernetes.io/name: {{ include "eveys-ocpp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "eveys-ocpp.gatewayLabels" -}}
{{ include "eveys-ocpp.commonLabels" . }}
app.kubernetes.io/component: gateway
{{- end -}}

{{- define "eveys-ocpp.envoyLabels" -}}
{{ include "eveys-ocpp.commonLabels" . }}
app.kubernetes.io/component: envoy
{{- end -}}

{{- define "eveys-ocpp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "eveys-ocpp.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "eveys-ocpp.envoyUpstreamHost" -}}
{{- if .Values.envoy.upstreamHost -}}
{{- .Values.envoy.upstreamHost -}}
{{- else -}}
{{- printf "%s.%s.svc.cluster.local" (include "eveys-ocpp.gatewayName" .) .Release.Namespace -}}
{{- end -}}
{{- end -}}
