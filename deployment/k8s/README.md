# 🚀 Platform Deployment & Infrastructure

This directory contains the Infrastructure as Code (IaC) required to orchestrate the Entertainment Data Platform. It provides a seamless transition from local development to a production-grade containerized environment.

---

## 📂 Directory Structure

- **`./docker`**: Contains `docker-compose.yml` for rapid local prototyping and integrated testing.
- **`./k8s`**: Kubernetes manifests organized into layers (`base`, `storage`, `services`) for scalable deployment.

---

## 🏗️ Deployment Strategies

### 1. Local Development (Docker Compose)
Ideal for developers who need to run the entire stack on a local machine. <br>
See more in the [Main Project README](../README.md).

```bash
cd deployment/docker
docker compose up -d
```

### 2. Production Cluster (Kubernetes)
The Kubernetes configuration is divided into three logical folders. To ensure stability, you must apply the manifests **file by file** in the specific order listed below.

#### **Step 1: Foundational Infrastructure (`base/`)**
Establishes the namespace and disk management policies.

```bash
kubectl apply -f k8s/base/01-namespace.yaml
kubectl apply -f k8s/base/02-storageclass.yaml
```

#### **Step 2: Persistent Storage Layer (`storage/`)**
Deploys the databases and object storage required for data persistence.

```bash
kubectl apply -f k8s/storage/01-minio.yaml
kubectl apply -f k8s/storage/02-clickhouse.yaml
kubectl apply -f k8s/storage/03-redis.yaml
```

#### **Step 3: Application Services (`services/`)**
Deploys the core logic. **Ingestion** must be started before the **Stream Processor** to ensure the message broker is ready to handle traffic.

```bash
# 01. Deploy Message Broker
kubectl apply -f k8s/services/01-kafka.yaml

# 02. Deploy Data Ingestion (Producers)
kubectl apply -f k8s/services/02-ingestion.yaml

# 03. Deploy Spark Stream Processing
kubectl apply -f k8s/services/03-stream-processor.yaml
```

---

## 🚦 Monitoring

### Check Status
```bash
kubectl get pods -n <namespace>
```

### Quick Logs
```bash
# Get logs from a specific pod
kubectl logs -f <pod-name> -n <namespace>

# Get logs from a deployment
kubectl logs -f deployment/<deployment-name> -n <namespace>
```

---