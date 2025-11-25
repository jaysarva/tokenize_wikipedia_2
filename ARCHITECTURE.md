# Wikipedia Tokenizer System Architecture

This document explains how the distributed Wikipedia tokenization system works, its failure resilience mechanisms, and potential improvements.

---

## System Overview

The system fetches Wikipedia articles via the MediaWiki API, tokenizes them using OpenAI's `tiktoken` library, and outputs token counts and token IDs. It runs on Kubernetes using Ray for distributed task execution.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Kubernetes Cluster                            │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                      wiki-tokenizer namespace                     │  │
│  │                                                                   │  │
│  │   ┌─────────────┐         ┌─────────────┐                        │  │
│  │   │  ConfigMap  │         │     PVC     │                        │  │
│  │   │ (page list) │         │ wiki-output │                        │  │
│  │   └──────┬──────┘         └──────┬──────┘                        │  │
│  │          │ /data                 │ /output                       │  │
│  │          ▼                       ▼                               │  │
│  │   ┌──────────────────────────────────────────┐                   │  │
│  │   │              Ray Head Pod                │                   │  │
│  │   │  ┌────────────────────────────────────┐  │                   │  │
│  │   │  │         Driver Process             │  │                   │  │
│  │   │  │  • Reads page list from /data      │  │                   │  │
│  │   │  │  • Schedules Ray tasks             │  │                   │  │
│  │   │  │  • Collects results                │  │                   │  │
│  │   │  │  • Writes output to /output        │  │                   │  │
│  │   │  └────────────────────────────────────┘  │                   │  │
│  │   │                    │                     │                   │  │
│  │   │           Ray Object Store               │                   │  │
│  │   │                    │                     │                   │  │
│  │   └────────────────────┼─────────────────────┘                   │  │
│  │                        │                                         │  │
│  │          ┌─────────────┼─────────────┐                           │  │
│  │          ▼             ▼             ▼                           │  │
│  │   ┌───────────┐ ┌───────────┐ ┌───────────┐                      │  │
│  │   │  Worker   │ │  Worker   │ │  Worker   │                      │  │
│  │   │   Pod     │ │   Pod     │ │   Pod     │                      │  │
│  │   │           │ │           │ │           │                      │  │
│  │   │ fetch +   │ │ fetch +   │ │ fetch +   │                      │  │
│  │   │ tokenize  │ │ tokenize  │ │ tokenize  │                      │  │
│  │   └───────────┘ └───────────┘ └───────────┘                      │  │
│  │          │             │             │                           │  │
│  └──────────┼─────────────┼─────────────┼───────────────────────────┘  │
│             │             │             │                              │
│             ▼             ▼             ▼                              │
│        ┌─────────────────────────────────────┐                         │
│        │        Wikipedia API (external)     │                         │
│        └─────────────────────────────────────┘                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### 1. Input

Page titles are loaded from a ConfigMap mounted at `/data/sample_pages.txt`:

```
OpenAI
Ray (distributed computing)
Kubernetes
Python (programming language)
...
```

### 2. Task Distribution

The driver on the head pod:
1. Reads all page titles
2. Creates Ray remote tasks for each page
3. Maintains a sliding window of `--concurrency` in-flight tasks
4. Waits for results via `ray.wait()`

```python
@ray.remote(num_cpus=0.25, max_retries=3, retry_exceptions=True)
def fetch_and_tokenize(title, endpoint, encoding_name, user_agent, persist_dir, tokens_dir):
    text = fetch_plaintext(title, endpoint, user_agent)
    token_ids = tokenize_text(text, encoding_name)
    return {"title": title, "token_count": len(token_ids), "status": "ok", ...}
```

### 3. Task Execution (on workers)

Each task:
1. Fetches article plaintext from Wikipedia API (with retries + exponential backoff)
2. Tokenizes text using `tiktoken` (cl100k_base encoding)
3. Optionally writes per-page token file to `--tokens-dir`
4. Returns result dict to driver

### 4. Output

The driver writes all results to the PVC:

```
/output/
├── token_counts.jsonl       # Summary: one JSON line per page
└── tokens/
    ├── OpenAI_a1b2c3d4.json
    ├── Kubernetes_e5f6g7h8.json
    └── ...
```

Each line in `token_counts.jsonl`:
```json
{"title": "OpenAI", "token_count": 12847, "chars": 54321, "duration_sec": 1.234, "status": "ok", "tokens_path": "/output/tokens/OpenAI_a1b2c3d4.json"}
```

---

## Failure Resilience

### Current Protections

| Component | Failure Mode | Protection | Recovery |
|-----------|--------------|------------|----------|
| **Wikipedia API** | 5xx, 429, timeout | 4 retries with exponential backoff (0.5s → 5s) | Automatic |
| **Ray Task** | Exception during fetch/tokenize | `max_retries=3` on `@ray.remote` | Ray reschedules on any worker |
| **Worker Pod** | OOM, crash, eviction | Ray detects and reschedules pending tasks | Automatic (in-flight task lost, retried) |
| **Output Data** | Pod restart, job TTL cleanup | PVC mounted at `/output` | Data persists on disk |
| **Minikube Stop** | Local dev shutdown | PVC backed by host storage | Data survives restart |

### Task-Level Retries

```python
@ray.remote(num_cpus=0.25, max_retries=3, retry_exceptions=True)
def fetch_and_tokenize(...):
```

- Ray automatically retries failed tasks up to 3 times
- `retry_exceptions=True` means any exception triggers a retry
- Failed tasks are rescheduled on any available worker

### API-Level Retries

```python
def fetch_plaintext(title, endpoint, user_agent, max_attempts=4):
    backoff = 0.5
    for attempt in range(max_attempts):
        try:
            resp = session.get(endpoint, params=params, timeout=15)
            if resp.status_code >= 500 or resp.status_code == 429:
                raise WikipediaFetchError(...)
            ...
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)
```

- 4 attempts per API call
- Exponential backoff: 0.5s → 1s → 2s → 4s (capped at 5s)
- Handles 5xx errors and rate limiting (429)

### Persistent Output

```yaml
# k8s/pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: wiki-output
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
```

- Output written to PVC, not ephemeral `/tmp`
- Survives: pod restarts, job TTL cleanup (1 hour), minikube stop
- Lost only on: `kubectl delete pvc` or `minikube delete`

### Graceful Error Handling

Tasks that fail after all retries still return a result (not crash):

```python
except Exception as exc:
    row = {
        "title": title,
        "status": "error",
        "error": str(exc),
        "duration_sec": round(duration, 3),
    }
    return row  # Job continues, error logged
```

- Job completes even with partial failures
- Final summary shows `ok` vs `errors` count
- Failed pages documented in output JSONL

---

## Current Limitations

### 1. Head Pod is a Single Point of Failure

**Problem:** If the head pod dies mid-job:
- In-flight tasks in Ray's object store are lost
- Driver state (which pages processed) is lost
- Partial results on PVC survive, but job must restart from scratch

**Impact:** For a 20-page toy job, this is minor. For 6M pages, restarting from zero is costly.

### 2. ~~No Checkpointing / Resume~~ (RESOLVED)

The driver now writes a checkpoint file (`/output/checkpoint.json`) listing completed pages. On restart, it loads the checkpoint and skips already-completed titles.

### 3. ~~PVC is ReadWriteOnce~~ (RESOLVED)

PVC now uses `ReadWriteMany` and workers mount `/output`. In cloud clusters with RWX-capable storage (EFS, Filestore, Azure Files), workers can write directly to storage.

### 4. ~~No Deduplication on Restart~~ (RESOLVED)

Tasks now check if output already exists before processing. Restarting a job skips pages that have already been tokenized.

### 5. Concurrency Bounded by API Rate Limits

**Problem:** Wikipedia rate-limits requests; aggressive concurrency causes 429 errors.

**Impact:** Can't fully utilize a large cluster against the live API.

---

## Recommended Improvements

### Tier 1: Quick Wins (Hours)

#### 1.1 Idempotent Task Execution (IMPLEMENTED)

Tasks now check if output already exists before processing:

```python
@ray.remote(num_cpus=0.25, max_retries=3, retry_exceptions=True)
def fetch_and_tokenize(title, endpoint, encoding_name, user_agent, persist_dir, tokens_dir):
    # Skip if output already exists (idempotent task execution)
    if tokens_dir:
        output_path = Path(tokens_dir) / _slug_filename(title)
        if output_path.exists():
            return {"title": title, "status": "skipped", "reason": "output_exists", ...}
    
    # ... existing logic ...
```

**Benefit:** Restarting a failed job skips already-processed pages. Progress logging and summary now show skipped count.

#### 1.2 Progress Checkpointing (IMPLEMENTED)

The driver now writes a checkpoint file listing completed pages:

```python
def save_checkpoint(checkpoint_path, completed_titles):
    data = {"completed": completed_titles, "count": len(completed_titles), "timestamp": time.time()}
    # Atomic write via tmp file + rename
    tmp.write_text(json.dumps(data))
    tmp.replace(path)

def load_checkpoint(checkpoint_path) -> Set[str]:
    # Returns set of completed titles from checkpoint file
    ...
```

On restart, the driver loads the checkpoint and filters out already-completed titles before processing:

```python
already_completed = load_checkpoint(args.checkpoint)
titles = [t for t in titles if t not in already_completed]
```

Enable via `--checkpoint /output/checkpoint.json` (already configured in `rayjob-toy.yaml`).

**Benefit:** Resume from last checkpoint instead of from zero.

#### 1.3 ReadWriteMany PVC (IMPLEMENTED)

PVC now uses `ReadWriteMany` access mode and workers mount the shared volume:

```yaml
# k8s/pvc.yaml
spec:
  accessModes: [ReadWriteMany]
  # For cloud clusters, set storageClassName:
  #   AWS EKS: efs-sc | GKE: filestore-sc | Azure: azurefile | On-prem: nfs-client

# k8s/rayjob-toy.yaml - workers now mount /output
volumeMounts:
  - name: wiki-output
    mountPath: /output
```

**Note:** Minikube's default provisioner only supports ReadWriteOnce. For true RWX in minikube, use the NFS addon. In cloud clusters, use the appropriate CSI driver (EFS, Filestore, Azure Files).

**Benefit:** Workers can write directly to storage; reduces driver bottleneck.

---

### Tier 2: Medium Effort (Days)

#### 2.1 External Manifest + Object Storage

Instead of a flat page list, use a manifest stored in object storage:

```json
{
  "job_id": "enwiki-20240101",
  "tasks": [
    {"task_id": "0001", "pages": ["OpenAI", "Kubernetes", ...], "status": "pending"},
    {"task_id": "0002", "pages": ["Python", "Ray", ...], "status": "pending"},
    ...
  ]
}
```

Tasks check/update their status in the manifest. Enables:
- Parallel job submitters
- External progress tracking
- Easy resume after failures

#### 2.2 Use CirrusSearch Dumps Instead of Live API

Download Wikipedia's pre-built text dumps:

```
https://dumps.wikimedia.org/other/cirrussearch/
```

**Benefits:**
- No API rate limits
- Faster (local I/O vs network)
- Reproducible (process same snapshot)
- Can process millions of pages without throttling

#### 2.3 Shard Jobs by Dump File

Instead of one giant job, submit one RayJob per dump file:

```
wiki-rayjob-enwiki-001.yaml  →  processes dump chunk 1
wiki-rayjob-enwiki-002.yaml  →  processes dump chunk 2
...
```

**Benefits:**
- Smaller blast radius on failure
- Independent progress tracking
- Easier parallelization across clusters

---

### Tier 3: Production Hardening (Weeks)

#### 3.1 External Job Submitter with Retry Logic

A separate Deployment or CronJob that:
1. Monitors RayJob status
2. Resubmits failed jobs automatically
3. Updates manifest with progress
4. Sends alerts on repeated failures

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: wiki-job-monitor
spec:
  schedule: "*/5 * * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: monitor
            image: tokenize-wiki:prod
            command: ["python", "-m", "ray_app.job_monitor"]
```

#### 3.2 Head Pod HA (Experimental)

Ray supports GCS fault tolerance with external Redis:

```yaml
rayStartParams:
  redis-address: "redis-ha:6379"
```

This allows head pod recovery without losing cluster state. (Requires careful configuration.)

#### 3.3 Observability Stack

- **Metrics:** Prometheus scraping Ray's `/metrics` endpoint
- **Dashboards:** Grafana showing task throughput, error rates, latency
- **Logs:** Centralized logging (Loki, ELK) for driver + workers
- **Alerts:** PagerDuty/Slack on job failure, high error rate, OOMs

---

## Summary: Failure Matrix

| Failure | Current Behavior | Remaining Improvements |
|---------|------------------|------------------------|
| Wikipedia API error | 4 retries → task fails → logged in output | - |
| Worker pod crash | Ray retries task on another worker | - |
| Head pod crash | **Resumes from checkpoint** (1.2 implemented) | - |
| Job restarted | **Skips existing outputs** (idempotent) + **checkpoint resume** | - |
| Minikube stop | Data survives on PVC | - |
| Minikube delete | Data lost | Use external object storage |

---

## Quick Reference: Key Files

| File | Purpose |
|------|---------|
| `ray_app/tokenize_wiki.py` | Main Ray job logic |
| `k8s/rayjob-toy.yaml` | RayJob manifest (head + workers) |
| `k8s/pvc.yaml` | Persistent storage for outputs |
| `k8s/pages-configmap.yaml` | Input page list |
| `scripts/run_job.sh` | Full dev cycle: deploy → wait → retrieve |
| `scripts/watch_job.sh` | Poll job until completion |

