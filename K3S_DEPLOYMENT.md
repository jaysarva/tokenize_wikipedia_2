# K3s Cluster Deployment Guide

End-to-end guide for deploying the Wikipedia Tokenizer on a K3s cluster.

## Prerequisites

- K3s cluster with all nodes joined and Ready
- `kubectl` access to the cluster
- Helm installed on the master node
- Docker installed on your local machine (for building images)
- Docker Hub account (or other container registry)

### Cluster Setup Used in This Guide

| Node | Role | External IP | Internal IP |
|------|------|-------------|-------------|
| 64-181-251-75 | control-plane, master | 64.181.251.75 | 10.19.49.195 |
| 163-192-5-72 | worker | 163.192.5.72 | 10.19.60.242 |
| 64-181-226-252 | worker | 64.181.226.252 | 10.19.50.137 |

---

## Step 1: Set Up NFS Server (on Master Node)

NFS provides ReadWriteMany storage so all pods can access the output directory.

```bash
# SSH to master node
ssh ubuntu@64-181-251-75

# Install NFS server
sudo apt update
sudo apt install -y nfs-kernel-server

# Create export directory
sudo mkdir -p /srv/nfs/wiki-output
sudo chown nobody:nogroup /srv/nfs/wiki-output
sudo chmod 777 /srv/nfs/wiki-output

# Configure exports
echo "/srv/nfs/wiki-output *(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports

# Apply and restart
sudo exportfs -ra
sudo systemctl restart nfs-kernel-server
sudo systemctl enable nfs-kernel-server

# Verify
exportfs -v
```

---

## Step 2: Install NFS Client on Worker Nodes

```bash
# SSH to each worker node and run:
sudo apt install -y nfs-common
```

---

## Step 3: Install Helm (if not already installed)

```bash
# On master node
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

---

## Step 4: Install NFS CSI Driver

```bash
# Add Helm repo
helm repo add csi-driver-nfs https://raw.githubusercontent.com/kubernetes-csi/csi-driver-nfs/master/charts
helm repo update

# Install CSI driver
helm install csi-driver-nfs csi-driver-nfs/csi-driver-nfs --namespace kube-system

# Create StorageClass (use master's internal IP)
kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: nfs-csi
provisioner: nfs.csi.k8s.io
parameters:
  server: "10.19.49.195"
  share: "/srv/nfs/wiki-output"
reclaimPolicy: Retain
volumeBindingMode: Immediate
mountOptions:
  - nfsvers=4.1
EOF

# Verify
kubectl get storageclass nfs-csi
```

---

## Step 5: Install KubeRay Operator

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --namespace ray-system --create-namespace

# Verify
kubectl get pods -n ray-system
```

---

## Step 6: Build and Push Docker Image

**Important:** Build for `linux/amd64` platform since K3s nodes run on x86_64.

```bash
# On your local machine (Mac/Linux with Docker)
cd /path/to/tokenize_wikipedia_2

# Build for linux/amd64 and push
docker buildx build --platform linux/amd64 -t docker.io/YOUR_USERNAME/tokenize-wiki:v1 --push .

# Example:
docker buildx build --platform linux/amd64 -t docker.io/jaysarva027/tokenize-wiki:v1 --push .
```

**Note:** If building on Apple Silicon Mac, you must use `--platform linux/amd64` or the image won't run on x86_64 nodes.

---

## Step 7: Update Kubernetes Manifests

Update the image reference in the manifests:

```bash
# Edit k8s/rayjob-toy.yaml and k8s/raycluster.yaml
# Change: image: "docker.io/DOCKERHUB_USERNAME/tokenize-wiki:v1"
# To:     image: "docker.io/YOUR_USERNAME/tokenize-wiki:v1"
```

Or use sed:
```bash
sed -i 's/DOCKERHUB_USERNAME/jaysarva027/g' k8s/rayjob-toy.yaml k8s/raycluster.yaml
```

Ensure `k8s/pvc.yaml` has:
```yaml
storageClassName: nfs-csi
accessModes:
  - ReadWriteMany
```

---

## Step 8: Copy Manifests to Master Node

```bash
# From your local machine
scp -r k8s/ ubuntu@64-181-251-75:~/wiki-tokenizer/tokenize_wikipedia_2/
```

Or clone the repo directly on the master node.

---

## Step 9: Deploy the Job

```bash
# On master node
cd ~/wiki-tokenizer/tokenize_wikipedia_2

# Create namespace
kubectl create ns wiki-tokenizer

# Apply resources
kubectl apply -n wiki-tokenizer -f k8s/pvc.yaml
kubectl apply -n wiki-tokenizer -f k8s/pages-configmap.yaml
kubectl apply -n wiki-tokenizer -f k8s/rayjob-toy.yaml
```

---

## Step 10: Monitor the Job

```bash
# Watch pods start up
kubectl get pods -n wiki-tokenizer -w

# Check RayJob status
kubectl get rayjob -n wiki-tokenizer

# View job submitter logs (shows progress)
kubectl logs -n wiki-tokenizer -l job-name=wiki-rayjob-sample

# View head pod logs
kubectl logs -n wiki-tokenizer -l ray.io/node-type=head --tail=100
```

---

## Step 11: Check Output

Once the job completes:

```bash
# Check output from a running pod
kubectl exec -it -n wiki-tokenizer <head-pod-name> -- cat /output/token_counts.jsonl

# Or find files on NFS (may be in a subdirectory)
find /srv/nfs/wiki-output -name "*.jsonl" 2>/dev/null
find /srv/nfs/wiki-output -type f 2>/dev/null
```

---

## Cleanup

```bash
# Delete the job (keeps PVC data)
kubectl delete rayjob wiki-rayjob-sample -n wiki-tokenizer

# Delete everything including data
kubectl delete ns wiki-tokenizer

# Uninstall KubeRay
helm uninstall kuberay-operator -n ray-system
kubectl delete ns ray-system

# Uninstall NFS CSI
helm uninstall csi-driver-nfs -n kube-system
kubectl delete storageclass nfs-csi
```

---

## Re-running Jobs

To run again with fresh data:

```bash
# Delete existing job
kubectl delete rayjob wiki-rayjob-sample -n wiki-tokenizer

# Clear checkpoint (optional - to reprocess all pages)
kubectl exec -it -n wiki-tokenizer <any-pod> -- rm -f /output/checkpoint.json

# Redeploy
kubectl apply -n wiki-tokenizer -f k8s/rayjob-toy.yaml
```

---

## Troubleshooting

### Image Pull Errors

**"no match for platform in manifest"**
- Rebuild image with `--platform linux/amd64`

**"ErrImagePull" or "ImagePullBackOff"**
- Verify image is public on Docker Hub
- Or create a pull secret for private images

### PVC Not Binding

```bash
kubectl get pvc -n wiki-tokenizer
kubectl describe pvc wiki-output -n wiki-tokenizer
```

Check that:
- NFS server is running: `systemctl status nfs-kernel-server`
- NFS client installed on workers: `apt install nfs-common`
- StorageClass exists: `kubectl get storageclass nfs-csi`

### Job Fails Immediately

```bash
# Check job submitter pod logs
kubectl logs -n wiki-tokenizer -l job-name=wiki-rayjob-sample
```

### Workers Not Connecting

```bash
# Check worker pod logs
kubectl logs -n wiki-tokenizer <worker-pod-name>

# Verify Ray cluster status from head
kubectl exec -it -n wiki-tokenizer <head-pod> -- ray status
```

---

## Quick Reference

| Command | Purpose |
|---------|---------|
| `kubectl get pods -n wiki-tokenizer` | List pods |
| `kubectl get rayjob -n wiki-tokenizer` | Check job status |
| `kubectl logs -n wiki-tokenizer <pod>` | View pod logs |
| `kubectl delete rayjob wiki-rayjob-sample -n wiki-tokenizer` | Delete job |
| `kubectl exec -it -n wiki-tokenizer <pod> -- bash` | Shell into pod |

---

## Files Reference

| File | Purpose |
|------|---------|
| `k8s/pvc.yaml` | Persistent storage (NFS) |
| `k8s/pages-configmap.yaml` | Input page list |
| `k8s/rayjob-toy.yaml` | RayJob definition (head + workers) |
| `k8s/rayjob-embed.yaml` | GPU embedding RayJob |
| `k8s/raycluster.yaml` | Standalone RayCluster (alternative) |
| `k8s/nfs-storageclass.yaml` | NFS StorageClass (reference) |
| `k8s/nvidia-device-plugin.yaml` | NVIDIA GPU plugin (reference) |
| `data/sample_pages.txt` | Sample Wikipedia pages to tokenize |

---

# GPU Embedding Pipeline

After tokenization, generate embeddings using GPU-accelerated sentence-transformers.

## GPU Prerequisites

- NVIDIA GPUs on cluster nodes
- NVIDIA drivers installed (`nvidia-smi` works)
- NVIDIA Container Toolkit configured for K3s

---

## GPU Step 1: Install NVIDIA Drivers (if needed)

```bash
# Check if GPU is detected
lspci | grep -i nvidia

# Install drivers
sudo apt-get update
sudo apt-get install -y nvidia-driver-535

# Reboot required after driver install
sudo reboot

# Verify after reboot
nvidia-smi
```

---

## GPU Step 2: Install NVIDIA Container Toolkit

Run on each GPU node:

```bash
# Add NVIDIA repo
distribution=$(. /etc/os-release; echo $ID$VERSION_ID)
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
    sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list | \
    sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
    sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

# Install toolkit
sudo apt-get update
sudo apt-get install -y nvidia-container-toolkit

# Configure for K3s (uses different containerd path)
sudo nvidia-ctk runtime configure --runtime=containerd \
    --config=/var/lib/rancher/k3s/agent/etc/containerd/config.toml

# Restart K3s
sudo systemctl restart k3s        # on master
sudo systemctl restart k3s-agent  # on workers
```

Or use the setup script:
```bash
sudo ./scripts/setup_nvidia_k3s.sh
```

---

## GPU Step 3: Install NVIDIA Device Plugin

Run once from any node with kubectl access:

```bash
kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.14.1/nvidia-device-plugin.yml

# Or use local copy
kubectl apply -f k8s/nvidia-device-plugin.yaml
```

Verify GPUs are detected:
```bash
kubectl get nodes -o json | jq '.items[].status.allocatable["nvidia.com/gpu"]'
# Should show "1" (or more) for each GPU node
```

---

## GPU Step 4: Build GPU Docker Image

```bash
# On your local machine
cd /path/to/tokenize_wikipedia_2

# Build GPU image with CUDA and sentence-transformers
docker buildx build --platform linux/amd64 \
    -f Dockerfile.gpu \
    -t docker.io/jaysarva027/tokenize-wiki:gpu-v1 \
    --push .
```

**Note:** This image is larger (~6GB) due to PyTorch + CUDA.

---

## GPU Step 5: Run Tokenization First

Ensure tokenization has completed before running embeddings:

```bash
# Run tokenization job (saves text to /output/tokens/)
kubectl apply -n wiki-tokenizer -f k8s/rayjob-toy.yaml

# Wait for completion
kubectl get rayjob -n wiki-tokenizer -w

# Verify tokens exist
kubectl exec -it -n wiki-tokenizer <pod> -- ls /output/tokens/
```

---

## GPU Step 6: Run Embedding Job

```bash
kubectl apply -n wiki-tokenizer -f k8s/rayjob-embed.yaml

# Monitor
kubectl get pods -n wiki-tokenizer -w
kubectl logs -n wiki-tokenizer -l ray.io/node-type=head -f
```

---

## GPU Step 7: Check Embedding Output

```bash
# List embeddings
kubectl exec -it -n wiki-tokenizer <pod> -- ls /output/embeddings/

# Check index file
kubectl exec -it -n wiki-tokenizer <pod> -- cat /output/embedding_index.jsonl

# Sample embedding (numpy format)
kubectl exec -it -n wiki-tokenizer <pod> -- python -c "
import numpy as np
emb = np.load('/output/embeddings/<filename>.npy')
print(f'Shape: {emb.shape}, First 5 values: {emb[:5]}')"
```

---

## Full Pipeline (Tokenize + Embed)

```bash
# 1. Tokenization
kubectl delete rayjob wiki-rayjob-sample -n wiki-tokenizer --ignore-not-found
kubectl apply -n wiki-tokenizer -f k8s/rayjob-toy.yaml
# Wait for completion...

# 2. Embedding
kubectl delete rayjob wiki-rayjob-embed -n wiki-tokenizer --ignore-not-found
kubectl apply -n wiki-tokenizer -f k8s/rayjob-embed.yaml
# Wait for completion...

# 3. Check results
kubectl exec -it -n wiki-tokenizer <pod> -- ls -la /output/
```

---

## GPU Troubleshooting

### "nvidia.com/gpu" not showing in allocatable

```bash
# Check device plugin pods
kubectl get pods -n kube-system | grep nvidia

# Check plugin logs
kubectl logs -n kube-system -l name=nvidia-device-plugin-ds

# Verify containerd config
cat /var/lib/rancher/k3s/agent/etc/containerd/config.toml | grep nvidia
```

### CUDA errors in pods

```bash
# Verify NVIDIA runtime is configured
sudo nvidia-ctk runtime configure --runtime=containerd \
    --config=/var/lib/rancher/k3s/agent/etc/containerd/config.toml
sudo systemctl restart k3s-agent  # or k3s on master
```

### Driver/library version mismatch

```bash
# Usually fixed by reboot
sudo reboot

# If persists, reinstall driver
sudo apt-get purge nvidia-*
sudo apt-get install -y nvidia-driver-535
sudo reboot
```

---

## Output Structure

After both jobs complete:

```
/output/
├── token_counts.jsonl       # Tokenization summary
├── checkpoint.json          # Tokenization checkpoint
├── tokens/                  # Per-page token files (with text)
│   ├── OpenAI_abc123.json
│   ├── Kubernetes_def456.json
│   └── ...
├── embeddings/              # Per-page embedding files
│   ├── OpenAI_abc123.npy    # 384-dim numpy array
│   ├── Kubernetes_def456.npy
│   └── ...
├── embedding_index.jsonl    # Embedding metadata
└── embed_checkpoint.json    # Embedding checkpoint
```

