{{- define "eyvan.name" -}}
{{- default "eyvan" .Values.nameOverride -}}
{{- end -}}

{{- define "eyvan.namespace" -}}
{{- .Values.namespace.name -}}
{{- end -}}

{{- define "eyvan.controlPlaneLabels" -}}
app: eyvan-control-plane
app.kubernetes.io/name: {{ include "eyvan.name" . }}
app.kubernetes.io/component: control-plane
{{- end -}}

{{- define "eyvan.cellAgentLabels" -}}
app: eyvan-cell-agent
app.kubernetes.io/name: {{ include "eyvan.name" . }}
app.kubernetes.io/component: cell-agent
{{- end -}}
