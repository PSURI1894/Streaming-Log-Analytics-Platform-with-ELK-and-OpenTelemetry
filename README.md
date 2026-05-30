# Streaming Log Analytics Platform with ELK and OpenTelemetry

An enterprise-grade, high-throughput streaming observability and telemetry platform ingesting, processing, and correlating logs, metrics, and traces from distributed microservices.

## System Architecture

The architecture implements a hybrid modern telemetry stack:
1. **Application Tier**: FastAPI Order & Payment services instrumented with the OpenTelemetry (OTel) SDK.
2. **Collection DaemonSet (Vector/OTel Agent)**: Scrapes structured logs and gathers host/pod metrics.
3. **Gateway Cluster (OTel Collector)**: Performs batching, high-risk PII redaction, and tail-based sampling.
4. **Buffering Tier (Kafka)**: Telemetry streams are separated into topics (`otel-logs`, `otel-metrics`, `otel-traces`) to buffer backends against spike overloads.
5. **Storage Tier (Loki, Tempo, Mimir, Elasticsearch)**:
   - **Loki**: Low-cardinality label indexing, storing raw logs in S3/MinIO chunks.
   - **Tempo**: Low-cost trace storage indexed by `trace_id` only.
   - **Mimir**: Long-term metric storage with multi-zone compactor nodes.
   - **Elasticsearch**: Full-text indexed backup logs.
6. **Query & Visualization (Grafana)**: Unified dashboard linking metrics, trace spans, and log lines.

## Components & Configuration

All configurations, Helm charts, and compose stacks are organized in:
- `apps/`: FastAPI Services & load generator.
- `deployments/`: Local docker-compose environment and Kubernetes Helm charts.
- `monitoring/`: Alertmanager configurations, SLO burn-rate alerts, and Grafana dashboards.
- `scripts/`: Operational tools including cardinality guards and context propagators.

## Local Quickstart

To boot up the complete mock environment locally:
```bash
docker-compose -f deployments/docker-compose/docker-compose.yml up -d
```
Access points:
- Grafana: `http://localhost:3000` (admin/admin)
- MinIO Dashboard: `http://localhost:9001` (minioadmin/minioadmin)
- Order Service API: `http://localhost:8080`
