# Production Deployment Guide

This guide provides comprehensive instructions for deploying the Data Pipeline Framework in production environments with enterprise-grade security, monitoring, and reliability.

## 🚀 Quick Start Checklist

- [ ] Environment setup complete
- [ ] Security configuration reviewed
- [ ] Database initialized
- [ ] Monitoring stack deployed
- [ ] Backup system configured
- [ ] SSL certificates installed
- [ ] CI/CD pipeline configured
- [ ] Load testing completed
- [ ] Disaster recovery plan documented

## 📋 Prerequisites

### Hardware Requirements

**Minimum Production Environment:**
- **Application Servers**: 4 CPU cores, 16GB RAM, 100GB SSD
- **Database Server**: 8 CPU cores, 32GB RAM, 500GB SSD with high IOPS
- **Redis Cache**: 2 CPU cores, 8GB RAM, 50GB SSD
- **Load Balancer**: 2 CPU cores, 4GB RAM
- **Monitoring Stack**: 4 CPU cores, 16GB RAM, 200GB SSD

**Recommended High-Availability Environment:**
- **Application Cluster**: 3+ nodes, 8 CPU cores each, 32GB RAM, 200GB SSD
- **Database Cluster**: 3 nodes (primary + 2 replicas), 16 CPU cores each, 64GB RAM, 1TB NVMe SSD
- **Redis Cluster**: 3 nodes, 4 CPU cores each, 16GB RAM
- **Load Balancers**: 2+ nodes (HA setup)
- **Monitoring Cluster**: Dedicated monitoring infrastructure

### Software Dependencies

- **Container Runtime**: Docker 20.10+ or Kubernetes 1.20+
- **Operating System**: Ubuntu 20.04 LTS, RHEL 8+, or Amazon Linux 2
- **Database**: PostgreSQL 15+ with extensions
- **Cache**: Redis 7+
- **Load Balancer**: NGINX 1.20+ or AWS ALB/ELB
- **Monitoring**: Prometheus, Grafana, ELK Stack

## 🔐 Security Configuration

### 1. SSL/TLS Setup

```bash
# Generate SSL certificates (for testing)
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout config/ssl/key.pem \
    -out config/ssl/cert.pem \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=data-pipeline.local"

# Set proper permissions
chmod 600 config/ssl/key.pem
chmod 644 config/ssl/cert.pem
```

### 2. Environment Variables

Create `.env` file with production settings:

```bash
# Database Configuration
DB_HOST=postgres-primary.internal
DB_PORT=5432
DB_NAME=data_warehouse_prod
DB_USER=pipeline_prod
DB_PASSWORD=<strong-random-password>

# Redis Configuration
REDIS_URL=redis://redis-cluster.internal:6379/0

# Security Settings
JWT_SECRET_KEY=<256-bit-random-key>
ENCRYPTION_KEY=<fernet-key>
ADMIN_USERNAME=admin
ADMIN_PASSWORD=<strong-admin-password>
ADMIN_EMAIL=admin@yourcompany.com

# AWS Configuration (if using S3)
AWS_ACCESS_KEY_ID=<access-key>
AWS_SECRET_ACCESS_KEY=<secret-key>
AWS_DEFAULT_REGION=us-east-1
S3_BACKUP_BUCKET=your-backup-bucket

# Monitoring
METRICS_ENABLED=true
LOG_LEVEL=INFO
ENVIRONMENT=production

# Backup Configuration
BACKUP_RETENTION_DAYS=90
BACKUP_SCHEDULE=0 2 * * *
```

### 3. Docker Secrets

```bash
# Create Docker secrets for sensitive data
echo "production_db_password" | docker secret create db_password -
echo "production_admin_secret" | docker secret create app_secret_key -
echo "grafana_admin_password" | docker secret create grafana_password -
echo "aws_credentials_file_content" | docker secret create aws_credentials -
```

## 🗄️ Database Setup

### 1. PostgreSQL Configuration

Create optimized `postgresql.conf`:

```ini
# Connection settings
max_connections = 200
shared_buffers = 8GB
effective_cache_size = 24GB
work_mem = 64MB
maintenance_work_mem = 2GB

# Write-ahead logging
wal_buffers = 16MB
checkpoint_completion_target = 0.9
checkpoint_timeout = 10min
max_wal_size = 4GB
min_wal_size = 1GB

# Performance tuning
random_page_cost = 1.1
effective_io_concurrency = 200
default_statistics_target = 100

# Logging
log_statement = 'mod'
log_min_duration_statement = 1000
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on

# Archiving for backup
archive_mode = on
archive_command = 'cp %p /var/lib/postgresql/wal_archive/%f'
```

### 2. Database Initialization

```sql
-- Create production database and user
CREATE DATABASE data_warehouse_prod;
CREATE USER pipeline_prod WITH PASSWORD 'secure_password';

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE data_warehouse_prod TO pipeline_prod;

-- Create extensions
\c data_warehouse_prod;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS audit;

-- Grant schema permissions
GRANT ALL ON SCHEMA raw_data TO pipeline_prod;
GRANT ALL ON SCHEMA processed_data TO pipeline_prod;
GRANT ALL ON SCHEMA metadata TO pipeline_prod;
GRANT ALL ON SCHEMA audit TO pipeline_prod;
```

## 🚢 Container Deployment

### 1. Docker Swarm Deployment

```bash
# Initialize Docker Swarm
docker swarm init

# Deploy the stack
docker stack deploy -c docker-compose.production.yml data-pipeline

# Verify deployment
docker service ls
docker stack ps data-pipeline
```

### 2. Kubernetes Deployment

```bash
# Create namespace
kubectl create namespace data-pipeline

# Apply configurations
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmaps/
kubectl apply -f k8s/secrets/
kubectl apply -f k8s/storage/
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/services/
kubectl apply -f k8s/ingress/

# Verify deployment
kubectl get pods -n data-pipeline
kubectl get services -n data-pipeline
```

## 📊 Monitoring Setup

### 1. Prometheus Configuration

The monitoring stack includes:
- **Prometheus**: Metrics collection and alerting
- **Grafana**: Visualization dashboards
- **AlertManager**: Alert routing and notification
- **ELK Stack**: Log aggregation and analysis

### 2. Key Metrics to Monitor

**Application Metrics:**
- Pipeline success/failure rates
- Queue depth and processing times
- Data quality scores
- API response times and error rates

**Infrastructure Metrics:**
- CPU, memory, and disk usage
- Database connection pools and query performance
- Redis cache hit rates
- Network I/O and latency

**Security Metrics:**
- Failed authentication attempts
- API rate limiting triggers
- Security event frequencies
- SSL certificate expiration dates

### 3. Alert Rules

Critical alerts configured:
- Application down (immediate)
- Database connection failures (1 minute)
- High error rates (5 minutes)
- Resource exhaustion (5 minutes)
- Security incidents (immediate)
- Backup failures (immediate)

## 🔄 Backup and Recovery

### 1. Automated Backup Schedule

The backup system provides:
- **Full backups**: Daily at 2 AM
- **Incremental backups**: Every 6 hours
- **Archive backups**: Monthly for long-term retention
- **Point-in-time recovery**: Via WAL archiving

### 2. Backup Verification

```bash
# Run manual backup verification
docker exec data-pipeline-backup python scripts/backup_scheduler.py verify

# Test restore procedure (on test environment)
docker exec data-pipeline-backup python scripts/restore.py \
    --backup-id full_20231201_020000 \
    --target-db test_restore_db
```

### 3. Disaster Recovery Procedures

**RTO (Recovery Time Objective)**: 4 hours
**RPO (Recovery Point Objective)**: 1 hour

**Recovery Steps:**
1. Assess scope of failure
2. Activate backup infrastructure
3. Restore database from latest backup
4. Apply incremental backups/WAL files
5. Update DNS/load balancer configuration
6. Verify system functionality
7. Notify stakeholders

## 🚦 Load Balancing and High Availability

### 1. NGINX Configuration

The production NGINX setup provides:
- SSL termination
- Load balancing across application instances
- Rate limiting and DDoS protection
- Security headers
- Compression and caching

### 2. Health Checks

All services include comprehensive health checks:
- Database connectivity
- Redis availability
- External service dependencies
- Resource utilization thresholds

## 🔧 Performance Tuning

### 1. Database Optimization

```sql
-- Create indexes for common queries
CREATE INDEX CONCURRENTLY idx_queue_items_status ON queue_items(status);
CREATE INDEX CONCURRENTLY idx_pipeline_runs_timestamp ON pipeline_runs(created_at);
CREATE INDEX CONCURRENTLY idx_data_quality_table_date ON data_quality(table_name, check_date);

-- Analyze tables for query planner
ANALYZE;

-- Update statistics
UPDATE pg_stat_statements SET calls = 0;
```

### 2. Application Tuning

```yaml
# Worker configuration
WORKER_CONCURRENCY: 4
QUEUE_POLL_INTERVAL: 10
BATCH_SIZE: 1000
MAX_MEMORY_USAGE: "6GB"

# Cache configuration
CACHE_TTL: 3600
CACHE_MAX_SIZE: "2GB"
ENABLE_QUERY_CACHE: true
```

## 📈 Scaling Guidelines

### Horizontal Scaling Triggers

**Scale Out When:**
- CPU usage > 70% for 10 minutes
- Queue depth > 1000 items
- Response time > 2 seconds for 5 minutes
- Memory usage > 85%

**Scale In When:**
- CPU usage < 30% for 30 minutes
- Queue depth < 100 items
- All response times < 500ms for 30 minutes

### Vertical Scaling Recommendations

**Database Scaling:**
- Monitor connection pool utilization
- Scale CPU for compute-heavy operations
- Scale memory for large datasets
- Scale storage for data growth

**Application Scaling:**
- Increase memory for larger batch sizes
- Add CPU cores for parallel processing
- Scale based on concurrent user load

## 🛡️ Security Best Practices

### 1. Access Control

- Use role-based access control (RBAC)
- Implement principle of least privilege
- Regular access reviews and user audits
- Multi-factor authentication for admin accounts

### 2. Network Security

- VPC/network segmentation
- Security groups/firewall rules
- Private subnets for databases
- VPN access for administrative tasks

### 3. Data Protection

- Encryption at rest and in transit
- PII data masking and anonymization
- Regular security vulnerability scans
- Compliance with data protection regulations

## 🔍 Troubleshooting

### Common Issues and Solutions

**Issue: High memory usage**
```bash
# Check memory usage by component
docker stats
kubectl top pods -n data-pipeline

# Optimize batch sizes
export BATCH_SIZE=500
```

**Issue: Database connection errors**
```bash
# Check connection pool status
SELECT count(*) FROM pg_stat_activity;

# Increase connection pool size
export DB_POOL_SIZE=50
```

**Issue: Queue processing delays**
```bash
# Check worker status
curl -f http://localhost:8080/api/health

# Scale workers
docker service scale data-pipeline_data-pipeline-worker=5
```

## 📞 Support and Maintenance

### Regular Maintenance Tasks

**Daily:**
- Review monitoring dashboards
- Check alert status
- Verify backup completion

**Weekly:**
- Review security logs
- Update software dependencies
- Performance optimization review

**Monthly:**
- Security audit
- Disaster recovery testing
- Capacity planning review
- Documentation updates

### Emergency Contacts

- **On-call Engineer**: [Your contact information]
- **Database Administrator**: [DBA contact]
- **Security Team**: [Security contact]
- **Infrastructure Team**: [Infrastructure contact]

## 📚 Additional Resources

- [API Documentation](docs/api/README.md)
- [Architecture Overview](docs/architecture/README.md)
- [Monitoring Runbooks](docs/runbooks/)
- [Security Procedures](docs/security/)
- [Troubleshooting Guide](docs/troubleshooting/)

---

**Note**: This deployment guide provides enterprise-grade production setup. Always test thoroughly in a staging environment before deploying to production. Regular security assessments and compliance reviews are essential for maintaining a secure and reliable system.