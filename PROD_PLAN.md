# Wikipedia Tokenization – Productionization Plan

This checklist covers building a 3-node Kubernetes cluster, installing KubeRay, and productionizing the tokenizer to process all Wikipedia pages via CirrusSearch dumps.

## Cluster bring-up (3 nodes)
- [ ] Provision 3 VMs (4–8 vCPU, 16–32 GiB RAM each), install container runtime (containerd), kubeadm/kubelet/kubectl.
- [ ] On control plane (master), initialize:
  ```bash
  sudo kubeadm init --pod-network-cidr=10.244.0.0/16
  mkdir -p $HOME/.kube
  sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config
  sudo chown $(id -u):$(id -g) $HOME/.kube/config
  ```
- [ ] Install CNI on control plane (Flannel example):
  ```bash
  kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/master/Documentation/kube-flannel.yml
  ```
- [ ] Join workers using the token from `kubeadm init` (run on each worker):
  ```bash
  kubeadm token create --print-join-command   # run on control plane to get command
  # run the printed join command on each worker node
  ```
- [ ] Install a default storage class on control plane (bare metal example: local-path):
  ```bash
  kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml
  kubectl patch storageclass local-path -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
  ```
- [ ] Validate (control plane/workstation):
  ```bash
  kubectl get nodes
  kubectl get pods -A
  kubectl get storageclass
  ```

## Base platform
- [ ] Install Helm on your workstation if not present (macOS: `brew install helm`; Linux: download helm tarball).
- [ ] Add KubeRay repo and install operator:
  ```bash
  helm repo add kuberay https://ray-project.github.io/kuberay-helm/
  helm repo update
  kubectl create ns ray-system
  helm upgrade --install kuberay-operator kuberay/kuberay-operator -n ray-system
  ```
- [ ] Verify CRDs and operator:
  ```bash
  kubectl get crds | grep ray.io
  kubectl get pods -n ray-system
  ```

## Container image and registry
- [ ] Build locally:
  ```bash
  docker build -t docker.io/<user>/tokenize-wiki:prod .
  ```
- [ ] Push to registry reachable by cluster:
  ```bash
  docker push docker.io/<user>/tokenize-wiki:prod
  ```
- [ ] If private registry, create pull secret:
  ```bash
  kubectl create secret docker-registry regcred \
    --docker-server=docker.io \
    --docker-username=<user> \
    --docker-password=<password> \
    --docker-email=<email> \
    -n wiki-tokenizer
  ```
  and reference it in RayCluster/RayJob under `imagePullSecrets`.
- [ ] Update manifests (`k8s/raycluster.yaml`, `k8s/rayjob-toy.yaml`) to use `docker.io/<user>/tokenize-wiki:prod` (or your registry).

## Object storage and data access
- [ ] Choose storage (S3/GCS/MinIO). Create buckets/prefixes for:
  - Cirrus dumps input (or access public HTTP directly if allowed).
  - Manifest files (task definitions).
  - Output shards (per-task JSONL) and success markers.
- [ ] Configure auth (IRSA/Workload Identity/service accounts with secrets). Mount creds into Ray pods via env/volume.
- [ ] Decide access mode: stream `json.gz` from HTTP/S3, or stage dumps to PVC/shared FS.
- [ ] If using an external object store (S3/GCS/MinIO), configure access:
  - Provide credentials via pod env/secret or workload identity (cloud).
  - Use bucket paths:
    - Dumps: `s3://wiki-dumps/...`
    - Manifests: `s3://wiki-manifests/enwiki-YYYYMMDD/manifest.json`
    - Outputs: `s3://wiki-output/enwiki-YYYYMMDD/{task_id}.jsonl`

## Work sharding and idempotency
- [ ] Define manifest format (e.g., JSON array of tasks: `{task_id, dump_uri, start_doc, count}` or byte ranges).
- [ ] Write a manifest generator to chunk each Cirrus dump into manageable tasks (1k–5k docs per task).
- [ ] Make tasks idempotent: skip if output shard exists (e.g., `output_prefix/{task_id}.jsonl`).
- [ ] Persist per-article results to durable storage (`--output-dir` to a PVC or object store).
- [ ] Enable Ray task retries (already set to 3); keep tasks small to minimize redo.

## Ray job changes (code-level)
- [ ] Add Cirrus dump reader (stream `json.gz`, parse per-doc JSON, extract text field).
- [ ] Add manifest ingestion (from object storage) and schedule tasks from it.
- [ ] Parameters: `--manifest-uri`, `--output-prefix`, `--output-dir`, `--text-field` (e.g., `source_text`), `--max-docs-per-task`.
- [ ] Keep concurrency modest; set `num_cpus` per task and worker sizes accordingly.
- [ ] Make outputs keyed by page_id/title for deduplication.

## RayCluster sizing and reliability
- [ ] Head: 2–4 vCPU, 8–16 GiB RAM; add PDB (minAvailable=1), anti-affinity, nodeSelector if needed.
- [ ] Workers: size for CPU-bound tokenization; start with 4–8 vCPU, 16–32 GiB RAM; scale replicas gradually.
- [ ] Set Ray `object-store-memory` caps; tune `RAY_memory_usage_threshold`.
- [ ] Use a steady RayService or a RayCluster + external submitter (CronJob/Deployment) that resubmits on failure.

## Job submission and recovery
- [ ] External submitter watches RayJob status; on failure (e.g., head loss), resubmits remaining tasks (idempotent via manifest/output checks).
- [ ] Shorter jobs per shard set (e.g., per dump file) to bound blast radius.
- [ ] Optional: track progress in a small metadata store (e.g., S3 manifest of completed tasks).

## Observability
- [ ] Expose Ray dashboard (8265) via a Service + ingress/port-forward.
- [ ] Aggregate logs (driver/head/worker) to a centralized sink (ELK/Grafana Loki).
- [ ] Metrics: task counts, error rates, throughput; scrape Ray metrics (Prometheus) and set alerts.
- [ ] Track OOMs and retries; adjust concurrency/resources accordingly.

## Validation and rollout
- [ ] Dry run on a single Cirrus dump with a handful of tasks; verify outputs.
- [ ] Scale to one namespace (e.g., enwiki) before adding others.
- [ ] Load test to find safe concurrency and worker sizing.
- [ ] Document runbooks: resubmission procedure, cleaning failed tasks, scaling guidance.

## Cleanup and cost control
- [ ] TTL for RayJobs and logs.
- [ ] Lifecycle policies on output buckets.
- [ ] Autoscaling policies on workers if supported by your infra.
