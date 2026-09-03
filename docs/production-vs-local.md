# WikiPulse / NexusAI — Local Docker Compose vs. Enterprise Production Deployment

This document contrasts the current local Docker Compose reference implementation with an enterprise-grade cloud production architecture.

---

## 1. Architectural Comparison Matrix

| System Dimension | Current Reference Implementation (Local Docker) | Enterprise Cloud Production Deployment |
| :--- | :--- | :--- |
| **Orchestration** | Single-host Docker Compose (`docker-compose.yml`) | Managed Kubernetes (EKS/GKE) with Helm charts and KEDA autoscaling |
| **Kafka Cluster** | Single broker (`apache/kafka:3.7.0`, `replication_factor: 1`) | 3+ Broker KRaft Cluster (AWS MSK) with `replication_factor: 3`, `min.insync.replicas: 2` |
| **Kafka Client** | `confluent-kafka` with plaintext local network | `confluent-kafka` with TLS mutual authentication (mTLS) and SASL/SCRAM |
| **Database** | Single container PostgreSQL 16 + `pgvector` extension | Amazon Aurora PostgreSQL Multi-AZ with auto-scaling Read Replicas |
| **Database Pooling** | Asyncpg connection pool in application (`pool_size: 20`) | PgBouncer / AWS RDS Proxy connection pooling layer |
| **Redis** | Single container Redis 7 Alpine | Redis Sentinel / AWS ElastiCache Cluster with Multi-AZ automated failover |
| **SSE Broadcasting** | In-process bounded `asyncio.Queue` fanout | Distributed Redis Pub/Sub backplane routing to multiple FastAPI edge pods |
| **Embedding Compute**| Local CPU SentenceTransformers (`all-MiniLM-L6-v2`) | GPU-accelerated embedding inference cluster (Triton Inference Server) |
| **Secrets** | Local `.env` file | AWS Secrets Manager / HashiCorp Vault with dynamic IAM role rotation |
| **Ingress & Security**| Direct host port publishing (8000, 8080) | AWS ALB / Cloudflare WAF with DDoS protection, SSL termination, and rate limits |
| **Observability** | Prometheus scraping local `/metrics` endpoint | Prometheus + Grafana + OpenTelemetry distributed tracing (Jaeger/Datadog) |
