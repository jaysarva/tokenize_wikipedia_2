# CirrusSearch Dump Pipeline - Usage Guide

This guide covers deploying the Wikipedia tokenization pipeline using CirrusSearch dumps instead of the Live API. This approach is faster, has no rate limits, and can process millions of articles.

---

## Prerequisites

- Kubernetes cluster (K3s, EKS, GKE, or Minikube)
- `kubectl` configured to access your cluster
- Helm installed
- Docker Hub account (or other container registry)
- ~50GB storage for dumps + output

---

## Quick Start (TL;DR)

```bash
# 1. Setup (one-time)
kubectl create ns wiki-tokenizer
./scripts/install_kuberay.sh
kubectl apply -n wiki-tokenizer -f k8s/pvc.yaml
kubectl apply -n wiki-tokenizer -f k8s/pvc-dumps.yaml

# 2. Deploy CirrusSearch job (downloads ~40GB dump on first run)
kubectl apply -n wiki-tokenizer -f k8s/rayjob-cirrus.yaml

# 3. Monitor
kubectl get pods -n wiki-tokenizer -w
kubectl logs -n wiki-tokenizer -l ray.io/node-type=head -f
```

---

## Step-by-Step Setup

### 1. Create Namespace

```bash
kubectl create ns wiki-tokenizer
```

Verify:
```bash
kubectl get ns wiki-tokenizer
```

---

### 2. Install KubeRay Operator

KubeRay provides the CRDs for RayCluster and RayJob resources.

```bash
./scripts/install_kuberay.sh
```

Or manually:
```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
kubectl create ns ray-system
helm upgrade --install kuberay-operator kuberay/kuberay-operator -n ray-system
```

Verify:
```bash
kubectl get pods -n ray-system
kubectl get crd rayjobs.ray.io
```

---

### 3. Setup Storage

#### Option A: NFS Storage (K3s / On-Prem)

On your NFS server (usually the master node):
```bash
sudo ./scripts/setup_nfs_server.sh
```

Install NFS CSI driver:
```bash
NFS_SERVER=<master-node-ip> ./scripts/setup_nfs_csi.sh
```

#### Option B: Cloud Storage (EKS/GKE/AKS)

Edit `k8s/pvc.yaml` and `k8s/pvc-dumps.yaml` to use your cloud storage class:
- **AWS EKS**: `storageClassName: efs-sc`
- **GKE**: `storageClassName: filestore-sc`
- **Azure AKS**: `storageClassName: azurefile`

---

### 4. Create PVCs

```bash
# Output storage (tokens, checkpoints)
kubectl apply -n wiki-tokenizer -f k8s/pvc.yaml

# Dump storage (~50GB for enwiki)
kubectl apply -n wiki-tokenizer -f k8s/pvc-dumps.yaml
```

Verify:
```bash
kubectl get pvc -n wiki-tokenizer
```

Expected output:
```
NAME          STATUS   VOLUME   CAPACITY   ACCESS MODES   STORAGECLASS
wiki-output   Bound    ...      10Gi       RWX            nfs-csi
wiki-dumps    Bound    ...      50Gi       RWX            nfs-csi
```

---

### 5. Build and Push Docker Image (if needed)

If you've modified the code, rebuild the image:

```bash
# Build and push
IMAGE_REGISTRY=docker.io/yourusername ./scripts/build_and_push.sh

# Update the image in manifests
sed -i 's|jaysarva027|yourusername|g' k8s/rayjob-cirrus.yaml
```

---

### 6. Deploy the CirrusSearch Job

```bash
kubectl apply -n wiki-tokenizer -f k8s/rayjob-cirrus.yaml
```

**What happens:**
1. Init container downloads the latest enwiki CirrusSearch dump (~40GB, takes 30-60 min)
2. Ray cluster starts (1 head + 4 workers)
3. Driver streams articles from dump and distributes tokenization tasks
4. Results are written to `/output/tokens/` on the PVC

---

### 7. Monitor Progress

#### Watch pods:
```bash
kubectl get pods -n wiki-tokenizer -w
```

#### View driver logs (shows progress):
```bash
kubectl logs -n wiki-tokenizer -l ray.io/node-type=head -f
```

Example output:
```
[1000/6000000] ok=998 skip=0 err=2 last=Albert Einstein (ok)
[2000/6000000] ok=1997 skip=0 err=3 last=Machine learning (ok)
```

#### Check job status:
```bash
kubectl get rayjob wiki-cirrus -n wiki-tokenizer
```

#### Ray Dashboard:
```bash
kubectl port-forward -n wiki-tokenizer svc/wiki-cirrus-head-svc 8265:8265
# Open http://localhost:8265
```

---

### 8. Retrieve Results

#### Check output files:
```bash
# Get a shell in the head pod
kubectl exec -it -n wiki-tokenizer \
  $(kubectl get pod -n wiki-tokenizer -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}') \
  -- ls -la /output/

# View token counts summary
kubectl exec -it -n wiki-tokenizer \
  $(kubectl get pod -n wiki-tokenizer -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}') \
  -- head /output/token_counts.jsonl
```

#### Copy results locally:
```bash
# Copy summary
kubectl cp wiki-tokenizer/$(kubectl get pod -n wiki-tokenizer -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}'):/output/token_counts.jsonl ./token_counts.jsonl

# Copy all tokens (large!)
kubectl cp wiki-tokenizer/$(kubectl get pod -n wiki-tokenizer -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}'):/output/tokens ./tokens/
```

---

## Resume After Failure

The job is **idempotent** and supports **checkpointing**:

1. **Checkpoint file** (`/output/checkpoint.json`) tracks completed page IDs
2. **Output files** are checked before processing (skip if exists)

To resume a failed job:
```bash
# Delete the failed job (keeps PVC data)
kubectl delete rayjob wiki-cirrus -n wiki-tokenizer

# Resubmit - will resume from checkpoint
kubectl apply -n wiki-tokenizer -f k8s/rayjob-cirrus.yaml
```

---

## Local Development & Testing

### Download a test dump locally:
```bash
# Small test dump (~250KB)
./scripts/download_dumps.sh ./dumps simplewikibooks

# Simple Wikipedia (~600MB)
./scripts/download_dumps.sh ./dumps simplewiki
```

### Test the reader:
```bash
python -m ray_app.cirrus_reader ./dumps/simplewikibooks*.json.gz --max 10
```

### Test tokenization locally:
```bash
# Activate venv
source .venv/bin/activate

# Run with local Ray
python -c "
import ray
ray.init()
from ray_app.tokenize_wiki import main
" 

# Or use the CLI (requires Ray cluster)
python -m ray_app.tokenize_wiki \
  --cirrus-file ./dumps/simplewikibooks*.json.gz \
  --output /tmp/token_counts.jsonl \
  --tokens-dir /tmp/tokens \
  --max-pages 100 \
  --concurrency 4
```

---

## Configuration Options

### Job Parameters (in `k8s/rayjob-cirrus.yaml`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--cirrus-dir` | `/dumps` | Directory with dump files |
| `--concurrency` | `8` | Parallel tokenization tasks |
| `--max-pages` | (none) | Limit articles processed |
| `--checkpoint` | `/output/checkpoint.json` | Resume checkpoint file |
| `--tokens-dir` | `/output/tokens` | Per-article token output |

### Scaling Workers

Edit `k8s/rayjob-cirrus.yaml`:
```yaml
workerGroupSpecs:
  - groupName: cirrus-workers
    replicas: 4        # Increase for more parallelism
    minReplicas: 2
    maxReplicas: 8
```

### Memory Tuning

For large dumps, increase memory:
```yaml
resources:
  requests:
    memory: "8Gi"
  limits:
    memory: "16Gi"
```

---

## Cleanup

### Delete job (keep data):
```bash
kubectl delete rayjob wiki-cirrus -n wiki-tokenizer
```

### Delete everything:
```bash
kubectl delete ns wiki-tokenizer
helm uninstall kuberay-operator -n ray-system
kubectl delete ns ray-system
```

### Delete PVC data:
```bash
kubectl delete pvc wiki-output wiki-dumps -n wiki-tokenizer
```

---

## Troubleshooting

### Init container stuck downloading
The enwiki dump is ~40GB. Check progress:
```bash
kubectl logs -n wiki-tokenizer \
  $(kubectl get pod -n wiki-tokenizer -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}') \
  -c download-dumps
```

### Workers OOMKilled
Reduce concurrency or increase memory limits:
```bash
# In rayjob-cirrus.yaml, lower concurrency
--concurrency 4

# Or increase worker memory
resources:
  limits:
    memory: "16Gi"
```

### PVC not binding
Check storage class exists:
```bash
kubectl get storageclass
```

For Minikube, comment out `storageClassName` in PVC yamls.

### Job stuck in PENDING
Check for resource constraints:
```bash
kubectl describe rayjob wiki-cirrus -n wiki-tokenizer
kubectl describe pod -n wiki-tokenizer -l ray.io/node-type=head
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     wiki-tokenizer namespace                     │
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     │
│  │  PVC: dumps  │     │ PVC: output  │     │   ConfigMap  │     │
│  │  (50Gi RWX)  │     │  (10Gi RWX)  │     │  (optional)  │     │
│  └──────┬───────┘     └──────┬───────┘     └──────────────┘     │
│         │                    │                                   │
│         │ /dumps             │ /output                           │
│         ▼                    ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                     Ray Head Pod                            │ │
│  │  ┌─────────────────┐  ┌─────────────────────────────────┐  │ │
│  │  │  Init Container │  │         Driver Process          │  │ │
│  │  │  (curl download)│  │  • Streams from /dumps/*.gz     │  │ │
│  │  │  enwiki dump    │  │  • Schedules tokenize tasks     │  │ │
│  │  │  (~40GB)        │  │  • Writes checkpoint + output   │  │ │
│  │  └─────────────────┘  └─────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                   │
│              ┌───────────────┼───────────────┐                  │
│              ▼               ▼               ▼                  │
│       ┌──────────┐    ┌──────────┐    ┌──────────┐             │
│       │  Worker  │    │  Worker  │    │  Worker  │             │
│       │  (CPU)   │    │  (CPU)   │    │  (CPU)   │             │
│       │ tokenize │    │ tokenize │    │ tokenize │             │
│       └──────────┘    └──────────┘    └──────────┘             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Comparison: Live API vs CirrusSearch

| Aspect | Live API | CirrusSearch Dumps |
|--------|----------|-------------------|
| Rate limits | Yes (429 errors) | None |
| Speed | ~1-2 pages/sec | ~100+ pages/sec |
| Data freshness | Real-time | Weekly snapshots |
| Setup complexity | Simple | Requires dump download |
| Storage needed | Minimal | ~50GB for enwiki |
| Reproducibility | No (content changes) | Yes (fixed snapshot) |

**Recommendation**: Use CirrusSearch for bulk processing (>1000 pages).

