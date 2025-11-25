# Wikipedia Tokenization with Ray on Kubernetes

This repo fetches Wikipedia pages, tokenizes them with OpenAI's `tiktoken`, and can scale from a small local toy run to a Ray cluster on Kubernetes (via KubeRay).

## Quick start: local toy run (10 pages)
1) Create a venv and install deps:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
Set a user-agent for Wikipedia (required to avoid 403s):
```bash
export WIKI_USER_AGENT="tokenize-wikipedia/0.1 (contact: you@example.com)"
```
2) Run against the sample page list in `data/sample_pages.txt`:
```bash
python toy_run.py --pages-file data/sample_pages.txt --output /tmp/token_counts.jsonl
```
3) You can override pages with `--pages "Python (programming language),Ray (distributed computing),Kubernetes"` or `--max-pages 5`. The script prints per-page token counts plus a total; errors are logged per-page and included in the output JSONL.

## Docker image (used by K8s/Ray)
Build and push your image so KubeRay can pull it:
```bash
docker build -t <registry>/tokenize-wiki:latest .
docker push <registry>/tokenize-wiki:latest
```
For Minikube you can skip pushing and use a local tag (see the Minikube section below).
If you rebuild the image, ensure you use the updated `requirements.txt` (now installs `ray[default]` so the dashboard health checks work) and the Dockerfile (installs bash, curl, wget).

## Kubernetes + KubeRay
Prereqs: `kubectl` pointing at your cluster, storage class for logs, and the KubeRay operator/CRDs installed (see https://docs.ray.io/en/latest/cluster/kubernetes/index.html).

1) Create a namespace:
```bash
kubectl create ns wiki-tokenizer
```
2) Apply a RayCluster sized for a small shakeout:
```bash
kubectl apply -n wiki-tokenizer -f k8s/raycluster.yaml
```
The manifest defaults to `tokenize-wiki:local` for Minikube testing; change to `<registry>/tokenize-wiki:latest` (or your registry tag) for remote clusters and tune resources as needed.
Update the `WIKI_USER_AGENT` env var in the manifests to include your contact info (Wikipedia requires a UA).

3) (Optional) Seed a ConfigMap with a toy page list for cluster jobs:
```bash
kubectl apply -n wiki-tokenizer -f k8s/pages-configmap.yaml
```

4) Submit a RayJob that runs `ray_app.tokenize_wiki` using the mounted page list:
```bash
kubectl apply -n wiki-tokenizer -f k8s/rayjob-toy.yaml
kubectl get pods -n wiki-tokenizer
kubectl logs -n wiki-tokenizer job-submit-rayjob-sample -f   # replace with the submitter pod name
```
The job writes to a PersistentVolumeClaim mounted at `/output`, so results persist across pod restarts and job cleanup.
If you change the user-agent value in `k8s/rayjob-toy.yaml`, keep it quoted because of the colon in the string.
If you see OOM errors, lower `--concurrency` (toy job uses 1), cap object store memory (set in manifests to 256Mi), raise the `RAY_memory_usage_threshold` env (0.99), or increase worker memory limits.
Output is written to a PVC for durability; results survive pod restarts and job TTL cleanup. Tasks retry up to 3 times on failure.
**Idempotent execution:** If you restart a failed job, pages that already have output files in `--tokens-dir` are automatically skipped (status: `skipped`). This allows resuming work without re-processing completed pages.

## K3s Cluster Deployment (Production)

For a multi-node K3s cluster (e.g., 1 master + 2 workers):

### Prerequisites
- K3s cluster with all nodes joined (`kubectl get nodes` shows Ready)
- Helm installed
- Docker Hub account (or other container registry)

### Setup Steps

1) **Set up NFS server on master node** (provides ReadWriteMany storage):
```bash
# SSH to master node and run:
sudo ./scripts/setup_nfs_server.sh

# Install NFS client on worker nodes:
sudo apt install -y nfs-common
```

2) **Install NFS CSI driver** (from any machine with kubectl access):
```bash
NFS_SERVER=10.19.49.195 ./scripts/setup_nfs_csi.sh  # Use master's internal IP
```

3) **Install KubeRay operator**:
```bash
./scripts/install_kuberay.sh
```

4) **Build and push Docker image**:
```bash
IMAGE_REGISTRY=docker.io/yourusername ./scripts/build_and_push.sh
```

5) **Update manifests** with your image registry:
```bash
# Edit k8s/rayjob-toy.yaml and k8s/raycluster.yaml
# Replace DOCKERHUB_USERNAME with your actual username
```

6) **Deploy the job**:
```bash
./scripts/deploy_toy_job.sh
```

7) **Monitor and retrieve results**:
```bash
kubectl get pods -n wiki-tokenizer -w
./scripts/watch_job.sh
./scripts/get_job_output.sh
```

### One-liner setup (after NFS server is running):
```bash
export NFS_SERVER=10.19.49.195
export IMAGE_REGISTRY=docker.io/yourusername
./scripts/setup_k3s_cluster.sh
./scripts/build_and_push.sh
./scripts/deploy_toy_job.sh
```

---

## Minikube + KubeRay toy run (local development)

Helper scripts in `scripts/` automate these steps.
1) Start Minikube with headroom:
```bash
scripts/minikube_start.sh
```
2) Install KubeRay operator (CRDs + controller):
```bash
scripts/install_kuberay.sh
```
3) Build the image inside Minikube's Docker (no push):
```bash
scripts/build_image_local.sh
```
If you changed `requirements.txt` or the Dockerfile (to include wget/bash) or tweaked defaults like `--concurrency`, rebuild before reapplying manifests.
4) Ensure manifests use the local image and your UA (already set to `tokenize-wiki:local`; update `WIKI_USER_AGENT` as needed).
5) Create namespace + ConfigMap and launch the toy RayJob (embedded cluster spec):
```bash
scripts/deploy_toy_job.sh
```
If you edit `data/sample_pages.txt`, this script reapplies `k8s/pages-configmap.yaml` so pods see the updated list.
This also creates a PVC (`wiki-output`) for persistent output storage.
6) Wait for job completion:
```bash
scripts/watch_job.sh        # polls until SUCCEEDED/FAILED (default 600s timeout)
scripts/watch_job.sh 300    # custom timeout
```
7) Inspect output:
```bash
scripts/get_job_output.sh
```
The driver logs emit progress lines like `[5/20] ok=4 err=1 last=Example (ok)` for quick status.
Tokens are written per-page when `--tokens-dir` is set (default in the toy job: `/tmp/tokens` on the driver pod).
To copy tokens locally:
```bash
LOCAL_DIR=./tokens scripts/get_tokens.sh
```
**One-liner for the full cycle** (deploy + wait + retrieve):
```bash
scripts/run_job.sh          # default 600s timeout
scripts/run_job.sh 300      # custom timeout
```
9) (Optional) Use the standalone RayCluster (`k8s/raycluster.yaml`) plus port-forward the dashboard:
```bash
kubectl apply -n wiki-tokenizer -f k8s/raycluster.yaml
kubectl port-forward svc/wiki-raycluster-head-svc 8265:8265 -n wiki-tokenizer
```
Or, for the auto-created RayJob cluster, port-forward the head service automatically:
```bash
scripts/port_forward_dashboard.sh   # then visit http://localhost:8265
```
10) Cleanup:
```bash
kubectl delete ns wiki-tokenizer
helm uninstall kuberay-operator -n ray-system
kubectl delete ns ray-system
minikube stop
```

## Scaling up
- **Page list:** Replace `k8s/pages-configmap.yaml` with your own list (newline-delimited titles). For millions of pages, store lists in object storage and change `--pages-file` to point at the mounted path or extend `ray_app/tokenize_wiki.py` to read from an object store iterator.
- **Workers:** Adjust the `RayCluster` worker group replicas and resource requests. Increase `--concurrency` in `ray_app/tokenize_wiki.py` cautiously to respect Wikipedia API rate limits or switch to page dumps to avoid throttling. Failed pages are reported but do not stop the job.
- **Fault tolerance:** `fetch_and_tokenize` has simple retries (up to 3 per task). Tasks are idempotent—if output already exists, the task is skipped. This allows safe job restarts without re-processing completed work.
- **Observability:** Ray dashboard runs on the head pod (port 8265). Port-forward for inspection: `kubectl port-forward svc/wiki-raycluster-head-svc 8265:8265 -n wiki-tokenizer`.

## Repo layout
- `toy_run.py` — Local single-process fetch + tokenize for a handful of pages.
- `ray_app/tokenize_wiki.py` — Ray job entrypoint for distributed fetch/tokenize.
- `data/sample_pages.txt` — Twenty sample titles for the toy run.
- `k8s/` — Example RayCluster and RayJob manifests; default to `tokenize-wiki:local` for Minikube, change to your registry tag for other clusters.
- `scripts/` — Helper scripts to start Minikube, install KubeRay, build the local image, deploy the toy RayJob, and grab results.
- `Dockerfile` — Image used by Ray pods/jobs.
