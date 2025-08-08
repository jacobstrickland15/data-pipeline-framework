#!/bin/bash
set -euo pipefail

# Data Pipeline Framework - Production Deployment Script
# This script handles deployment to various environments (Docker Swarm, Kubernetes, etc.)

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VERSION="${VERSION:-latest}"
ENVIRONMENT="${ENVIRONMENT:-production}"
DEPLOYMENT_TARGET="${DEPLOYMENT_TARGET:-docker-compose}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Data Pipeline Framework - Deployment Script

Usage: $0 [OPTIONS] COMMAND

COMMANDS:
    build           Build Docker images
    deploy          Deploy to target environment
    rollback        Rollback to previous version
    status          Check deployment status
    logs            View application logs
    scale           Scale services
    backup          Create database backup
    restore         Restore from backup
    health          Check system health
    cleanup         Cleanup old resources

OPTIONS:
    -e, --environment ENV     Target environment (production, staging, development)
    -t, --target TARGET       Deployment target (docker-compose, kubernetes, swarm)
    -v, --version VERSION     Version to deploy (default: latest)
    --dry-run                 Show what would be done without executing
    --force                   Force deployment without confirmations
    -h, --help               Show this help message

EXAMPLES:
    $0 build                                    # Build all images
    $0 deploy -e production -t kubernetes      # Deploy to Kubernetes production
    $0 scale data-pipeline-app 5               # Scale app to 5 replicas
    $0 rollback -v v1.2.0                      # Rollback to specific version
    $0 backup --compress                       # Create compressed database backup

EOF
}

# Validate environment
validate_environment() {
    log_info "Validating environment: $ENVIRONMENT"
    
    # Check required tools
    local required_tools=("docker" "docker-compose")
    if [[ "$DEPLOYMENT_TARGET" == "kubernetes" ]]; then
        required_tools+=("kubectl" "helm")
    fi
    
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            log_error "Required tool '$tool' is not installed"
            exit 1
        fi
    done
    
    # Check environment-specific requirements
    case "$ENVIRONMENT" in
        production)
            if [[ ! -f "$PROJECT_ROOT/.env.production" ]]; then
                log_error "Production environment file not found: .env.production"
                exit 1
            fi
            ;;
        staging)
            if [[ ! -f "$PROJECT_ROOT/.env.staging" ]]; then
                log_warning "Staging environment file not found, using defaults"
            fi
            ;;
    esac
    
    log_success "Environment validation completed"
}

# Build Docker images
build_images() {
    log_info "Building Docker images for version: $VERSION"
    
    cd "$PROJECT_ROOT"
    
    # Build main application image
    log_info "Building main application image..."
    docker build -f Dockerfile.production --target production -t "data-pipeline:$VERSION" .
    
    # Build web interface image
    log_info "Building web interface image..."
    docker build -f Dockerfile.production --target web -t "data-pipeline-web:$VERSION" .
    
    # Build worker image
    log_info "Building worker image..."
    docker build -f Dockerfile.production --target worker -t "data-pipeline-worker:$VERSION" .
    
    # Build monitoring dashboard image
    log_info "Building monitoring dashboard image..."
    docker build -f Dockerfile.production --target monitoring -t "data-pipeline-dashboard:$VERSION" .
    
    # Build backup service image
    log_info "Building backup service image..."
    docker build -f Dockerfile.production --target backup -t "data-pipeline-backup:$VERSION" .
    
    # Tag as latest if this is the latest version
    if [[ "$VERSION" != "latest" ]]; then
        docker tag "data-pipeline:$VERSION" "data-pipeline:latest"
        docker tag "data-pipeline-web:$VERSION" "data-pipeline-web:latest"
        docker tag "data-pipeline-worker:$VERSION" "data-pipeline-worker:latest"
        docker tag "data-pipeline-dashboard:$VERSION" "data-pipeline-dashboard:latest"
        docker tag "data-pipeline-backup:$VERSION" "data-pipeline-backup:latest"
    fi
    
    log_success "All images built successfully"
}

# Push images to registry
push_images() {
    local registry="${DOCKER_REGISTRY:-docker.io}"
    local namespace="${DOCKER_NAMESPACE:-data-pipeline}"
    
    log_info "Pushing images to registry: $registry/$namespace"
    
    local images=("data-pipeline" "data-pipeline-web" "data-pipeline-worker" "data-pipeline-dashboard" "data-pipeline-backup")
    
    for image in "${images[@]}"; do
        log_info "Pushing $image:$VERSION"
        docker tag "$image:$VERSION" "$registry/$namespace/$image:$VERSION"
        docker push "$registry/$namespace/$image:$VERSION"
        
        if [[ "$VERSION" != "latest" ]]; then
            docker tag "$image:$VERSION" "$registry/$namespace/$image:latest"
            docker push "$registry/$namespace/$image:latest"
        fi
    done
    
    log_success "All images pushed to registry"
}

# Deploy with Docker Compose
deploy_docker_compose() {
    log_info "Deploying with Docker Compose"
    
    cd "$PROJECT_ROOT"
    
    local compose_file="docker-compose.production.yml"
    if [[ "$ENVIRONMENT" != "production" ]]; then
        compose_file="docker-compose.${ENVIRONMENT}.yml"
        if [[ ! -f "$compose_file" ]]; then
            compose_file="docker-compose.yml"
        fi
    fi
    
    # Load environment variables
    if [[ -f ".env.$ENVIRONMENT" ]]; then
        set -a
        source ".env.$ENVIRONMENT"
        set +a
    fi
    
    # Create necessary directories
    mkdir -p data logs backups
    
    # Deploy services
    log_info "Starting services with $compose_file"
    docker-compose -f "$compose_file" up -d --remove-orphans
    
    # Wait for services to be ready
    wait_for_services_docker_compose
    
    log_success "Docker Compose deployment completed"
}

# Deploy with Kubernetes
deploy_kubernetes() {
    log_info "Deploying to Kubernetes"
    
    cd "$PROJECT_ROOT"
    
    # Apply namespace
    kubectl apply -f k8s/namespace.yaml
    
    # Apply ConfigMaps and Secrets
    kubectl apply -f k8s/configmap.yaml -n data-pipeline
    
    # Check if secrets exist, create if they don't
    if ! kubectl get secret database-credentials -n data-pipeline &> /dev/null; then
        log_info "Creating database credentials secret"
        kubectl create secret generic database-credentials \
            --from-literal=username="${DB_USER:-pipeline_user}" \
            --from-literal=password="${DB_PASSWORD}" \
            -n data-pipeline
    fi
    
    # Apply PersistentVolumeClaims
    kubectl apply -f k8s/pvc.yaml -n data-pipeline
    
    # Apply Services
    kubectl apply -f k8s/service.yaml -n data-pipeline
    
    # Apply Deployments
    kubectl apply -f k8s/deployment.yaml -n data-pipeline
    
    # Apply Ingress
    if [[ -f "k8s/ingress.yaml" ]]; then
        kubectl apply -f k8s/ingress.yaml -n data-pipeline
    fi
    
    # Apply HorizontalPodAutoscaler
    if [[ -f "k8s/hpa.yaml" ]]; then
        kubectl apply -f k8s/hpa.yaml -n data-pipeline
    fi
    
    # Wait for rollout to complete
    kubectl rollout status deployment/data-pipeline-app -n data-pipeline
    kubectl rollout status deployment/data-pipeline-worker -n data-pipeline
    kubectl rollout status deployment/postgres -n data-pipeline
    kubectl rollout status deployment/redis -n data-pipeline
    
    log_success "Kubernetes deployment completed"
}

# Wait for services to be ready (Docker Compose)
wait_for_services_docker_compose() {
    log_info "Waiting for services to be ready..."
    
    local services=("data-pipeline-app:8080" "postgres:5432" "redis:6379")
    local max_wait=300  # 5 minutes
    local wait_time=0
    
    for service in "${services[@]}"; do
        local service_name="${service%%:*}"
        local port="${service##*:}"
        
        log_info "Waiting for $service_name on port $port..."
        
        while ! docker-compose exec -T "$service_name" timeout 1 bash -c "</dev/tcp/localhost/$port" &> /dev/null; do
            if [[ $wait_time -ge $max_wait ]]; then
                log_error "Timeout waiting for $service_name"
                return 1
            fi
            sleep 5
            wait_time=$((wait_time + 5))
        done
        
        log_success "$service_name is ready"
    done
}

# Check deployment status
check_status() {
    log_info "Checking deployment status for $DEPLOYMENT_TARGET"
    
    case "$DEPLOYMENT_TARGET" in
        docker-compose)
            docker-compose -f docker-compose.production.yml ps
            ;;
        kubernetes)
            kubectl get pods -n data-pipeline
            kubectl get services -n data-pipeline
            kubectl get ingress -n data-pipeline 2>/dev/null || true
            ;;
        swarm)
            docker service ls
            ;;
    esac
}

# Scale services
scale_services() {
    local service="$1"
    local replicas="$2"
    
    log_info "Scaling $service to $replicas replicas"
    
    case "$DEPLOYMENT_TARGET" in
        docker-compose)
            docker-compose -f docker-compose.production.yml up -d --scale "$service=$replicas"
            ;;
        kubernetes)
            kubectl scale deployment "$service" --replicas="$replicas" -n data-pipeline
            ;;
        swarm)
            docker service scale "$service=$replicas"
            ;;
    esac
    
    log_success "Scaled $service to $replicas replicas"
}

# Create database backup
create_backup() {
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_name="data_warehouse_backup_${timestamp}"
    local compress_flag=""
    
    if [[ "${1:-}" == "--compress" ]]; then
        compress_flag="--compress"
        backup_name="${backup_name}.gz"
    fi
    
    log_info "Creating database backup: $backup_name"
    
    case "$DEPLOYMENT_TARGET" in
        docker-compose)
            docker-compose exec postgres pg_dump -U pipeline_user -d data_warehouse $compress_flag > "backups/$backup_name"
            ;;
        kubernetes)
            kubectl exec -n data-pipeline -c postgres deployment/postgres -- pg_dump -U pipeline_user -d data_warehouse $compress_flag > "backups/$backup_name"
            ;;
    esac
    
    log_success "Database backup created: backups/$backup_name"
}

# Restore from backup
restore_backup() {
    local backup_file="$1"
    
    if [[ ! -f "$backup_file" ]]; then
        log_error "Backup file not found: $backup_file"
        exit 1
    fi
    
    log_warning "This will overwrite the existing database. Are you sure? (y/N)"
    read -r confirmation
    if [[ "$confirmation" != "y" && "$confirmation" != "Y" ]]; then
        log_info "Restore cancelled"
        return 0
    fi
    
    log_info "Restoring database from: $backup_file"
    
    case "$DEPLOYMENT_TARGET" in
        docker-compose)
            if [[ "$backup_file" == *.gz ]]; then
                gunzip -c "$backup_file" | docker-compose exec -T postgres psql -U pipeline_user -d data_warehouse
            else
                docker-compose exec -T postgres psql -U pipeline_user -d data_warehouse < "$backup_file"
            fi
            ;;
        kubernetes)
            if [[ "$backup_file" == *.gz ]]; then
                gunzip -c "$backup_file" | kubectl exec -i -n data-pipeline -c postgres deployment/postgres -- psql -U pipeline_user -d data_warehouse
            else
                kubectl exec -i -n data-pipeline -c postgres deployment/postgres -- psql -U pipeline_user -d data_warehouse < "$backup_file"
            fi
            ;;
    esac
    
    log_success "Database restore completed"
}

# Rollback deployment
rollback_deployment() {
    local target_version="$1"
    
    log_info "Rolling back to version: $target_version"
    
    case "$DEPLOYMENT_TARGET" in
        kubernetes)
            kubectl rollout undo deployment/data-pipeline-app -n data-pipeline
            kubectl rollout undo deployment/data-pipeline-worker -n data-pipeline
            ;;
        docker-compose)
            VERSION="$target_version" docker-compose -f docker-compose.production.yml up -d
            ;;
    esac
    
    log_success "Rollback completed"
}

# Check system health
check_health() {
    log_info "Checking system health"
    
    # Check application health
    local app_url="http://localhost:8080/api/health"
    if curl -s -f "$app_url" > /dev/null; then
        log_success "Application health check passed"
    else
        log_error "Application health check failed"
    fi
    
    # Check database
    case "$DEPLOYMENT_TARGET" in
        docker-compose)
            if docker-compose exec postgres pg_isready -U pipeline_user -d data_warehouse > /dev/null; then
                log_success "Database health check passed"
            else
                log_error "Database health check failed"
            fi
            ;;
        kubernetes)
            if kubectl exec -n data-pipeline -c postgres deployment/postgres -- pg_isready -U pipeline_user -d data_warehouse > /dev/null; then
                log_success "Database health check passed"
            else
                log_error "Database health check failed"
            fi
            ;;
    esac
    
    # Check Redis
    case "$DEPLOYMENT_TARGET" in
        docker-compose)
            if docker-compose exec redis redis-cli ping | grep -q "PONG"; then
                log_success "Redis health check passed"
            else
                log_error "Redis health check failed"
            fi
            ;;
        kubernetes)
            if kubectl exec -n data-pipeline -c redis deployment/redis -- redis-cli ping | grep -q "PONG"; then
                log_success "Redis health check passed"
            else
                log_error "Redis health check failed"
            fi
            ;;
    esac
}

# View logs
view_logs() {
    local service="${1:-}"
    local lines="${2:-100}"
    
    case "$DEPLOYMENT_TARGET" in
        docker-compose)
            if [[ -n "$service" ]]; then
                docker-compose logs -f --tail="$lines" "$service"
            else
                docker-compose logs -f --tail="$lines"
            fi
            ;;
        kubernetes)
            if [[ -n "$service" ]]; then
                kubectl logs -f --tail="$lines" -n data-pipeline deployment/"$service"
            else
                kubectl logs -f --tail="$lines" -n data-pipeline -l app=data-pipeline-app
            fi
            ;;
    esac
}

# Cleanup old resources
cleanup_resources() {
    log_info "Cleaning up old resources"
    
    # Remove unused Docker images
    docker image prune -f
    
    # Remove unused volumes
    docker volume prune -f
    
    # Remove old backups (keep last 30)
    find backups/ -name "*.sql*" -type f -mtime +30 -delete 2>/dev/null || true
    
    log_success "Cleanup completed"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--environment)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -t|--target)
                DEPLOYMENT_TARGET="$2"
                shift 2
                ;;
            -v|--version)
                VERSION="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --force)
                FORCE=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                COMMAND="$1"
                shift
                break
                ;;
        esac
    done
    
    # Store additional arguments
    ARGS=("$@")
}

# Main execution
main() {
    parse_args "$@"
    
    if [[ -z "${COMMAND:-}" ]]; then
        show_help
        exit 1
    fi
    
    # Validate environment unless showing help
    validate_environment
    
    case "$COMMAND" in
        build)
            build_images
            ;;
        push)
            push_images
            ;;
        deploy)
            case "$DEPLOYMENT_TARGET" in
                docker-compose)
                    deploy_docker_compose
                    ;;
                kubernetes)
                    deploy_kubernetes
                    ;;
                *)
                    log_error "Unsupported deployment target: $DEPLOYMENT_TARGET"
                    exit 1
                    ;;
            esac
            ;;
        status)
            check_status
            ;;
        scale)
            if [[ ${#ARGS[@]} -lt 2 ]]; then
                log_error "Scale command requires service name and replica count"
                exit 1
            fi
            scale_services "${ARGS[0]}" "${ARGS[1]}"
            ;;
        backup)
            create_backup "${ARGS[0]:-}"
            ;;
        restore)
            if [[ ${#ARGS[@]} -lt 1 ]]; then
                log_error "Restore command requires backup file path"
                exit 1
            fi
            restore_backup "${ARGS[0]}"
            ;;
        rollback)
            if [[ ${#ARGS[@]} -lt 1 ]]; then
                log_error "Rollback command requires target version"
                exit 1
            fi
            rollback_deployment "${ARGS[0]}"
            ;;
        health)
            check_health
            ;;
        logs)
            view_logs "${ARGS[0]:-}" "${ARGS[1]:-100}"
            ;;
        cleanup)
            cleanup_resources
            ;;
        *)
            log_error "Unknown command: $COMMAND"
            show_help
            exit 1
            ;;
    esac
}

# Run main function
main "$@"