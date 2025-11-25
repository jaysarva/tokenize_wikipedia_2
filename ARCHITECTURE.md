# Wikipedia Processing Pipeline Architecture

A distributed system for tokenizing and embedding Wikipedia articles at scale, running on Kubernetes with Ray for distributed computing.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Data Pipeline](#data-pipeline)
3. [Kubernetes Architecture](#kubernetes-architecture)
4. [Component Deep Dive](#component-deep-dive)
5. [Failure Resilience](#failure-resilience)
6. [Strategies for Improvement](#strategies-for-improvement)
7. [Quick Reference](#quick-reference)

---

## System Overview

This system processes Wikipedia articles through a two-stage pipeline:

1. **Tokenization Stage** (CPU-bound): Extract and tokenize article text using OpenAI's `tiktoken` library
2. **Embedding Stage** (GPU-accelerated): Generate vector embeddings using `sentence-transformers`

The pipeline supports two input modes:
- **Live API Mode**: Fetch articles in real-time from Wikipedia's MediaWiki API
- **CirrusSearch Mode**: Stream pre-extracted text from Wikipedia's CirrusSearch dump files (recommended for bulk processing)

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                                    DATA FLOW                                          │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│   ┌─────────────────────┐         ┌─────────────────────┐         ┌────────────────┐ │
│   │    INPUT SOURCE     │         │   TOKENIZATION      │         │   EMBEDDING    │ │
│   │                     │         │                     │         │                │ │
│   │  ┌───────────────┐  │         │  ┌───────────────┐  │         │ ┌────────────┐ │ │
│   │  │ CirrusSearch  │──┼────────▶│  │   tiktoken    │──┼────────▶│ │ MiniLM     │ │ │
│   │  │ Dumps (.gz)   │  │         │  │  cl100k_base  │  │         │ │ GPU Model  │ │ │
│   │  └───────────────┘  │         │  └───────────────┘  │         │ └────────────┘ │ │
│   │        OR           │         │                     │         │                │ │
│   │  ┌───────────────┐  │         │  Output:            │         │  Output:       │ │
│   │  │ Wikipedia API │  │         │  • token_ids[]      │         │  • 384-dim     │ │
│   │  │ (live fetch)  │  │         │  • token_count      │         │    vectors     │ │
│   │  └───────────────┘  │         │  • text content     │         │  • .npy files  │ │
│   │                     │         │                     │         │                │ │
│   └─────────────────────┘         └─────────────────────┘         └────────────────┘ │
│                                                                                       │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### Why Two Stages?

| Aspect | Tokenization | Embedding |
|--------|--------------|-----------|
| **Compute Type** | CPU-bound | GPU-accelerated |
| **Resource Needs** | Low memory, I/O heavy | High VRAM, compute heavy |
| **Parallelism** | High (100s of concurrent tasks) | Limited by GPU count |
| **Bottleneck** | Network/disk I/O | GPU memory bandwidth |

Separating stages allows optimal resource allocation: scale CPU workers for tokenization, GPU workers for embedding.

---

## Data Pipeline

### Stage 1: Tokenization

Tokenization converts raw Wikipedia text into token IDs using OpenAI's `tiktoken` library with the `cl100k_base` encoding (same as GPT-4).

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           TOKENIZATION WORKFLOW                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   INPUT                          PROCESS                         OUTPUT              │
│   ─────                          ───────                         ──────              │
│                                                                                      │
│   CirrusSearch Dump              Ray Task Pool                   Per-Article JSON    │
│   ┌─────────────────┐            ┌─────────────────┐             ┌────────────────┐  │
│   │ {"index": ...}  │            │ @ray.remote     │             │ {              │  │
│   │ {"title": "AI", │───────────▶│ tokenize_article│────────────▶│   "title":     │  │
│   │  "text": "..."}│             │ (num_cpus=0.25) │             │   "page_id":   │  │
│   │ {"index": ...}  │            │                 │             │   "text":      │  │
│   │ {"title": "ML", │            │ • tiktoken enc  │             │   "tokens":    │  │
│   │  "text": "..."}│            │ • atomic write  │             │   "token_count"│  │
│   └─────────────────┘            └─────────────────┘             │ }              │  │
│                                                                   └────────────────┘  │
│   Stream: 6M+ articles           Concurrency: 8-64               /output/tokens/     │
│   ~40GB compressed               ~100 articles/sec               ~50GB uncompressed  │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Input Modes:**

| Mode | Source | Speed | Rate Limits | Use Case |
|------|--------|-------|-------------|----------|
| **CirrusSearch** | Pre-built dump files | ~100+ pages/sec | None | Bulk processing (>1000 pages) |
| **Live API** | Wikipedia MediaWiki API | ~1-2 pages/sec | Yes (429 errors) | Small jobs, real-time data |

**Output Format:**

Each article produces a JSON file in `/output/tokens/`:

```json
{
  "title": "Machine learning",
  "page_id": 233488,
  "text": "Machine learning (ML) is a field of study...",
  "tokens": [22287, 6975, 320, 2735, 8, 374, 264, 2115, ...],
  "token_count": 15847
}
```

### Stage 2: Embedding Generation

Embedding converts tokenized text into dense vector representations using GPU-accelerated sentence-transformers.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           EMBEDDING WORKFLOW                                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   INPUT                          PROCESS                         OUTPUT              │
│   ─────                          ───────                         ──────              │
│                                                                                      │
│   Token JSON Files               Ray GPU Task                    NumPy Arrays        │
│   ┌─────────────────┐            ┌─────────────────┐             ┌────────────────┐  │
│   │ /output/tokens/ │            │ @ray.remote     │             │ .npy files     │  │
│   │                 │            │ (num_gpus=1)    │             │                │  │
│   │ ├── AI_abc123   │───────────▶│                 │────────────▶│ 384-dim float  │  │
│   │ ├── ML_def456   │            │ SentenceTransf. │             │ vectors        │  │
│   │ └── ...         │            │ all-MiniLM-L6-v2│             │                │  │
│   │                 │            │                 │             │ + index.jsonl  │  │
│   └─────────────────┘            └─────────────────┘             └────────────────┘  │
│                                                                                      │
│   Read from Stage 1              Batch processing                /output/embeddings/ │
│   Validate: text, token_count    GPU memory optimized            ~5GB for 1M articles│
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Model: all-MiniLM-L6-v2**
- 384-dimensional embeddings
- Fast inference (~1000 sentences/sec on GPU)
- Good quality for semantic search

**Output Format:**

Each article produces:
1. `.npy` file containing the 384-dim float32 vector
2. Entry in `embedding_index.jsonl`:

```json
{"title": "Machine learning", "status": "ok", "embedding_dim": 384, "embedding_file": "/output/embeddings/Machine_learning_abc123.npy", "duration_sec": 0.045}
```

---

## Kubernetes Architecture

The system runs on Kubernetes using Ray for distributed task execution. The KubeRay operator manages Ray clusters as native Kubernetes resources.

### Cluster Topology

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              KUBERNETES CLUSTER                                         │
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │                          ray-system namespace                                      │ │
│  │  ┌─────────────────────────────────────────────────────────────────────────────┐  │ │
│  │  │                         KubeRay Operator                                     │  │ │
│  │  │  • Watches RayJob/RayCluster CRDs                                           │  │ │
│  │  │  • Creates/manages Ray head and worker pods                                 │  │ │
│  │  │  • Handles scaling and lifecycle                                            │  │ │
│  │  └─────────────────────────────────────────────────────────────────────────────┘  │ │
│  └───────────────────────────────────────────────────────────────────────────────────┘ │
│                                         │                                               │
│                                         │ manages                                       │
│                                         ▼                                               │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐ │
│  │                          wiki-tokenizer namespace                                  │ │
│  │                                                                                    │ │
│  │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                        │ │
│  │   │ PVC: dumps   │    │ PVC: output  │    │  ConfigMap   │                        │ │
│  │   │  (50Gi RWX)  │    │  (10Gi RWX)  │    │ (page list)  │                        │ │
│  │   │              │    │              │    │   optional   │                        │ │
│  │   └──────┬───────┘    └──────┬───────┘    └──────────────┘                        │ │
│  │          │ /dumps            │ /output                                             │ │
│  │          ▼                   ▼                                                     │ │
│  │   ┌────────────────────────────────────────────────────────────────────────────┐  │ │
│  │   │                          RayJob Resource                                    │  │ │
│  │   │                                                                             │  │ │
│  │   │   ┌─────────────────────────────────────────────────────────────────────┐  │  │ │
│  │   │   │                       RAY HEAD POD                                   │  │  │ │
│  │   │   │                                                                      │  │  │ │
│  │   │   │  ┌────────────────┐    ┌────────────────────────────────────────┐   │  │  │ │
│  │   │   │  │ Init Container │    │           Main Container               │   │  │  │ │
│  │   │   │  │                │    │                                        │   │  │  │ │
│  │   │   │  │ • Download     │    │  • Ray Head Process                    │   │  │  │ │
│  │   │   │  │   CirrusSearch │    │  • GCS (Global Control Store)          │   │  │  │ │
│  │   │   │  │   dumps (~40GB)│    │  • Dashboard (:8265)                   │   │  │  │ │
│  │   │   │  │ • One-time on  │    │  • Driver Process                      │   │  │  │ │
│  │   │   │  │   first run    │    │    - Reads input                       │   │  │  │ │
│  │   │   │  │                │    │    - Schedules Ray tasks               │   │  │  │ │
│  │   │   │  └────────────────┘    │    - Collects results                  │   │  │  │ │
│  │   │   │                        │    - Writes checkpoints                │   │  │  │ │
│  │   │   │                        └────────────────────────────────────────┘   │  │  │ │
│  │   │   │                                                                      │  │  │ │
│  │   │   │   Resources: 2-4 CPU, 4-8Gi RAM, optional GPU                       │  │  │ │
│  │   │   └──────────────────────────────────────────────────────────────────────┘  │  │ │
│  │   │                                        │                                    │  │ │
│  │   │                    ┌───────────────────┼───────────────────┐                │  │ │
│  │   │                    │                   │                   │                │  │ │
│  │   │                    ▼                   ▼                   ▼                │  │ │
│  │   │   ┌──────────────────────┐ ┌──────────────────┐ ┌──────────────────────┐   │  │ │
│  │   │   │   CPU WORKER POD     │ │  CPU WORKER POD  │ │    GPU WORKER POD    │   │  │ │
│  │   │   │                      │ │                  │ │                      │   │  │ │
│  │   │   │  • Tokenization      │ │  • Tokenization  │ │  • Embedding         │   │  │ │
│  │   │   │  • 0.25 CPU/task     │ │  • 0.25 CPU/task │ │  • 1 GPU/task        │   │  │ │
│  │   │   │  • Mounts /dumps     │ │  • Mounts /dumps │ │  • Mounts /output    │   │  │ │
│  │   │   │  • Mounts /output    │ │  • Mounts /output│ │  • sentence-transf.  │   │  │ │
│  │   │   │                      │ │                  │ │                      │   │  │ │
│  │   │   │  2-4 CPU, 4-8Gi RAM  │ │  2-4 CPU, 4-8Gi  │ │  2-4 CPU, 4-8Gi      │   │  │ │
│  │   │   │                      │ │                  │ │  1x NVIDIA GPU       │   │  │ │
│  │   │   └──────────────────────┘ └──────────────────┘ └──────────────────────┘   │  │ │
│  │   │                                                                             │  │ │
│  │   │   Worker Scaling: replicas 2-8 (auto-managed by KubeRay)                   │  │ │
│  │   └─────────────────────────────────────────────────────────────────────────────┘  │ │
│  │                                                                                    │ │
│  └────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Pod Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              POD LIFECYCLE FLOW                                          │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   1. JOB SUBMISSION                                                                      │
│   ─────────────────                                                                      │
│                                                                                          │
│   kubectl apply -f rayjob-cirrus.yaml                                                    │
│                    │                                                                     │
│                    ▼                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ KubeRay Operator detects new RayJob resource                                     │   │
│   │ Creates RayCluster with head + worker pods                                       │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                    │                                                                     │
│                    ▼                                                                     │
│   2. INIT PHASE (Head Pod Only)                                                          │
│   ─────────────────────────────                                                          │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Init Container: download-dumps                                                   │   │
│   │ ┌─────────────────────────────────────────────────────────────────────────────┐ │   │
│   │ │ if dump exists AND .complete marker exists:                                  │ │   │
│   │ │     skip download (already verified)                                         │ │   │
│   │ │ elif dump exists WITHOUT marker:                                             │ │   │
│   │ │     verify with gzip -t                                                      │ │   │
│   │ │     if valid → create marker, skip download (legacy file support)            │ │   │
│   │ │     if invalid → resume with curl -C -                                       │ │   │
│   │ │ else:                                                                        │ │   │
│   │ │     curl -L dumps.wikimedia.org/.../enwiki-*-cirrussearch-content.json.gz   │ │   │
│   │ │     verify gzip integrity (gzip -t)                                          │ │   │
│   │ │     create .complete marker file                                             │ │   │
│   │ │     # ~40GB, takes 30-60 minutes                                             │ │   │
│   │ └─────────────────────────────────────────────────────────────────────────────┘ │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                    │                                                                     │
│                    ▼                                                                     │
│   3. RAY CLUSTER FORMATION                                                               │
│   ────────────────────────                                                               │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Head Pod:                                                                        │   │
│   │   • Starts Ray head process (GCS, dashboard, object store)                      │   │
│   │   • Listens on port 6379 (GCS), 8265 (dashboard)                                │   │
│   │                                                                                  │   │
│   │ Worker Pods:                                                                     │   │
│   │   • Connect to head via ray-head-svc:6379                                       │   │
│   │   • Register available resources (CPU/GPU)                                      │   │
│   │   • Ready to receive tasks                                                      │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                    │                                                                     │
│                    ▼                                                                     │
│   4. JOB EXECUTION                                                                       │
│   ────────────────                                                                       │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ Driver (on head pod):                                                            │   │
│   │   • Loads checkpoint (if exists)                                                │   │
│   │   • Streams articles from /dumps/*.json.gz                                      │   │
│   │   • Submits Ray tasks with concurrency limit                                    │   │
│   │   • Collects results via ray.wait()                                             │   │
│   │   • Saves checkpoint every N completions                                        │   │
│   │   • Writes final output to /output/                                             │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                    │                                                                     │
│                    ▼                                                                     │
│   5. CLEANUP                                                                             │
│   ───────                                                                                │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ shutdownAfterJobFinishes: true                                                   │   │
│   │   • Ray cluster pods terminated                                                 │   │
│   │                                                                                  │   │
│   │ ttlSecondsAfterFinished: 3600                                                   │   │
│   │   • RayJob resource deleted after 1 hour                                        │   │
│   │   • PVC data persists (not deleted)                                             │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Storage Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              STORAGE ARCHITECTURE                                        │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                        PERSISTENT VOLUME CLAIMS                                  │   │
│   │                                                                                  │   │
│   │   ┌─────────────────────────────┐    ┌─────────────────────────────┐            │   │
│   │   │      wiki-dumps (50Gi)      │    │     wiki-output (10Gi)      │            │   │
│   │   │                             │    │                             │            │   │
│   │   │  Access: ReadWriteMany      │    │  Access: ReadWriteMany      │            │   │
│   │   │  StorageClass: nfs-csi      │    │  StorageClass: nfs-csi      │            │   │
│   │   │                             │    │                             │            │   │
│   │   │  Contents:                  │    │  Contents:                  │            │   │
│   │   │  /dumps/                    │    │  /output/                   │            │   │
│   │   │  └── enwiki-*-cirrussearch  │    │  ├── token_counts.jsonl     │            │   │
│   │   │      -content.json.gz       │    │  ├── checkpoint.json        │            │   │
│   │   │      (~40GB)                │    │  ├── tokens/                │            │   │
│   │   │                             │    │  │   ├── Article1_abc.json  │            │   │
│   │   │                             │    │  │   ├── Article2_def.json  │            │   │
│   │   │                             │    │  │   └── ...                │            │   │
│   │   │                             │    │  └── embeddings/            │            │   │
│   │   │                             │    │      ├── Article1_abc.npy   │            │   │
│   │   │                             │    │      ├── embedding_index.   │            │   │
│   │   │                             │    │      │   jsonl              │            │   │
│   │   │                             │    │      └── ...                │            │   │
│   │   └─────────────────────────────┘    └─────────────────────────────┘            │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   WHY ReadWriteMany (RWX)?                                                               │
│   ────────────────────────                                                               │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                                                                  │   │
│   │   ReadWriteOnce (RWO)              ReadWriteMany (RWX)                          │   │
│   │   ─────────────────────            ────────────────────                          │   │
│   │                                                                                  │   │
│   │   ┌─────────┐                      ┌─────────┐  ┌─────────┐  ┌─────────┐        │   │
│   │   │  Head   │──── /output          │  Head   │  │ Worker1 │  │ Worker2 │        │   │
│   │   └─────────┘     │                └────┬────┘  └────┬────┘  └────┬────┘        │   │
│   │                   │                     │            │            │              │   │
│   │   ┌─────────┐     │                     └────────────┴────────────┘              │   │
│   │   │ Worker  │     │                                  │                           │   │
│   │   │ (no     │     │                            /output (shared)                  │   │
│   │   │  mount) │     ▼                                  │                           │   │
│   │   └─────────┘    PVC                                 ▼                           │   │
│   │                                                     PVC                          │   │
│   │   Workers can't write                                                            │   │
│   │   directly to storage.              All pods can read/write.                     │   │
│   │   Driver bottleneck.                Workers write tokens directly.               │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   STORAGE CLASS OPTIONS                                                                  │
│   ─────────────────────                                                                  │
│                                                                                          │
│   │ Platform │ Storage Class │ Notes                                                    │
│   │──────────│───────────────│─────────────────────────────────────────                 │
│   │ K3s      │ nfs-csi       │ NFS server on master node                                │
│   │ AWS EKS  │ efs-sc        │ Elastic File System                                      │
│   │ GKE      │ filestore-sc  │ Cloud Filestore                                          │
│   │ Azure    │ azurefile     │ Azure Files                                              │
│   │ Minikube │ (default)     │ Only RWO; use NFS addon for RWX                          │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Deep Dive

### CirrusSearch Reader (`cirrus_reader.py`)

The CirrusSearch reader streams articles from Wikipedia's pre-built search index dumps, avoiding API rate limits entirely.

```python
# Dump format (NDJSON with alternating lines):
{"index": {"_type": "page", "_id": "12345"}}
{"page_id": 12345, "title": "Example", "text": "...", "namespace": 0, ...}
```

**Key Features:**
- **Streaming**: Memory-efficient iteration over 40GB+ files
- **Compression support**: Handles `.json.gz` and `.json.bz2`
- **Namespace filtering**: Only process main articles (namespace=0)
- **Error resilience**: Logs and skips malformed lines

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           CIRRUS READER FLOW                                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ┌──────────────────┐                                                                   │
│   │ enwiki-*-cirrus  │                                                                   │
│   │ search-content   │                                                                   │
│   │ .json.gz         │                                                                   │
│   │ (~40GB)          │                                                                   │
│   └────────┬─────────┘                                                                   │
│            │                                                                             │
│            ▼                                                                             │
│   ┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐          │
│   │  gzip.open()     │───────▶│  Read line pairs │───────▶│  JSON parse      │          │
│   │  streaming       │        │  (index + doc)   │        │  document        │          │
│   └──────────────────┘        └──────────────────┘        └────────┬─────────┘          │
│                                                                     │                    │
│                                                                     ▼                    │
│                                                            ┌──────────────────┐          │
│                                                            │ Namespace filter │          │
│                                                            │ (keep ns=0 only) │          │
│                                                            └────────┬─────────┘          │
│                                                                     │                    │
│                                                                     ▼                    │
│                                                            ┌──────────────────┐          │
│                                                            │ Yield article:   │          │
│                                                            │ {page_id, title, │          │
│                                                            │  text, namespace}│          │
│                                                            └──────────────────┘          │
│                                                                                          │
│   Statistics (English Wikipedia):                                                        │
│   • ~6.7 million articles                                                               │
│   • ~40GB compressed                                                                     │
│   • Processing rate: ~100-500 articles/sec                                              │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Tokenization Task (`tokenize_wiki.py`)

Each tokenization task is a Ray remote function that processes one article:

```python
@ray.remote(num_cpus=0.25, max_retries=3, retry_exceptions=True)
def tokenize_article(article, encoding_name, persist_dir, tokens_dir):
    # 1. Check if output exists (idempotent)
    if output_path.exists():
        return {"status": "skipped", "reason": "output_exists"}
    
    # 2. Tokenize with tiktoken
    token_ids = tiktoken.get_encoding(encoding_name).encode(text)
    
    # 3. Atomic write (tmp file + rename)
    tmp.write_text(json.dumps({...}))
    tmp.replace(output_path)
    
    return {"status": "ok", "token_count": len(token_ids)}
```

**Design Decisions:**

| Decision | Rationale |
|----------|-----------|
| `num_cpus=0.25` | Tokenization is fast; allows 4 tasks per CPU core |
| `max_retries=3` | Handles transient failures (network, OOM) |
| Atomic writes | Prevents partial files on crash |
| Idempotent check | Safe to restart; skips completed work |

### Embedding Task (`embed_wiki.py`)

Embedding tasks run on GPU workers with the sentence-transformers library:

```python
@ray.remote(num_gpus=1, max_retries=2, retry_exceptions=True)
def generate_embeddings_batch(articles, model_name, output_dir):
    # Load model once per batch (GPU memory optimization)
    model = SentenceTransformer(model_name, device="cuda")
    
    results = []
    for article in articles:
        # Skip if exists (idempotent)
        if output_path.exists():
            results.append({"status": "skipped"})
            continue
        
        # Generate embedding
        embedding = model.encode(text, convert_to_numpy=True)
        np.save(output_path, embedding)
        results.append({"status": "ok", "embedding_dim": len(embedding)})
    
    return results
```

**Batching Strategy:**

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           BATCH PROCESSING                                               │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   WITHOUT BATCHING                        WITH BATCHING (batch_size=10)                  │
│   ─────────────────                       ─────────────────────────────                  │
│                                                                                          │
│   Task 1: Load model → Embed → Unload     Task 1: Load model                             │
│   Task 2: Load model → Embed → Unload              → Embed article 1                     │
│   Task 3: Load model → Embed → Unload              → Embed article 2                     │
│   ...                                              → ...                                 │
│                                                    → Embed article 10                    │
│   Model loading: ~2-3 sec each                     → Unload                              │
│   10 articles = 10 × 2.5s = 25s overhead                                                │
│                                           Model loading: 2.5s once                       │
│                                           10 articles = 2.5s overhead                    │
│                                                                                          │
│   Speedup: ~10x reduction in model loading overhead                                      │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Pipeline Orchestrator (`pipeline.py`)

The pipeline script runs both stages in sequence within a single RayJob:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           PIPELINE EXECUTION                                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   python -m ray_app.pipeline                                                             │
│     --pages-file /data/sample_pages.txt                                                  │
│     --output-dir /output                                                                 │
│     --model all-MiniLM-L6-v2                                                             │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                                                                  │   │
│   │   STAGE 1: TOKENIZATION                                                         │   │
│   │   ─────────────────────                                                         │   │
│   │                                                                                  │   │
│   │   • Calls tokenize_wiki.main() internally                                       │   │
│   │   • Uses CPU workers (num_cpus=0.25 per task)                                   │   │
│   │   • Writes to /output/tokens/                                                   │   │
│   │   • Creates /output/tokenize_checkpoint.json                                    │   │
│   │                                                                                  │   │
│   │   if not token_files:                                                           │   │
│   │       sys.exit(1)  # Fail fast if tokenization produced nothing                │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                                │
│                                         ▼                                                │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                                                                  │   │
│   │   STAGE 2: EMBEDDING                                                            │   │
│   │   ──────────────────                                                            │   │
│   │                                                                                  │   │
│   │   • Calls embed_wiki.main() internally                                          │   │
│   │   • Uses GPU workers (num_gpus=1 per task)                                      │   │
│   │   • Reads from /output/tokens/                                                  │   │
│   │   • Writes to /output/embeddings/                                               │   │
│   │   • Creates /output/embed_checkpoint.json                                       │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                                │
│                                         ▼                                                │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                                                                  │   │
│   │   FINAL OUTPUT                                                                  │   │
│   │   ────────────                                                                  │   │
│   │                                                                                  │   │
│   │   /output/                                                                      │   │
│   │   ├── token_counts.jsonl          # Summary of tokenization                    │   │
│   │   ├── tokenize_checkpoint.json    # Tokenization progress                      │   │
│   │   ├── tokens/                     # Per-article token files                    │   │
│   │   │   ├── Article1_abc123.json                                                 │   │
│   │   │   └── ...                                                                  │   │
│   │   ├── embeddings/                 # Per-article embedding files                │   │
│   │   │   ├── Article1_abc123.npy                                                  │   │
│   │   │   └── ...                                                                  │   │
│   │   ├── embedding_index.jsonl       # Summary of embeddings                      │   │
│   │   └── embed_checkpoint.json       # Embedding progress                         │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Failure Resilience

The system is designed to handle failures at every level, from transient network errors to complete node crashes.

### Failure Matrix

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           FAILURE HANDLING MATRIX                                        │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   FAILURE TYPE              DETECTION              RECOVERY                              │
│   ────────────              ─────────              ────────                              │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ NETWORK / API ERRORS                                                             │   │
│   │                                                                                  │   │
│   │   Wikipedia API 5xx     HTTP status code       Exponential backoff retry         │   │
│   │   Wikipedia API 429     HTTP status code       Wait 0.5s → 1s → 2s → 4s          │   │
│   │   Connection timeout    requests.Timeout       Retry up to 4 attempts            │   │
│   │   DNS failure           requests.Exception     Retry with backoff                │   │
│   │                                                                                  │   │
│   │   Code:                                                                          │   │
│   │   ┌──────────────────────────────────────────────────────────────────────────┐  │   │
│   │   │ backoff = 0.5                                                             │  │   │
│   │   │ for attempt in range(4):                                                  │  │   │
│   │   │     try:                                                                  │  │   │
│   │   │         resp = session.get(endpoint, timeout=15)                          │  │   │
│   │   │         if resp.status_code >= 500 or resp.status_code == 429:            │  │   │
│   │   │             raise WikipediaFetchError(...)                                │  │   │
│   │   │     except Exception:                                                     │  │   │
│   │   │         time.sleep(backoff)                                               │  │   │
│   │   │         backoff = min(backoff * 2, 5.0)                                   │  │   │
│   │   └──────────────────────────────────────────────────────────────────────────┘  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ RAY TASK FAILURES                                                                │   │
│   │                                                                                  │   │
│   │   Task exception        Ray task monitoring    max_retries=3 on @ray.remote      │   │
│   │   Worker OOM            Ray worker death       Reschedule on another worker      │   │
│   │   Task timeout          Ray timeout            Automatic retry                   │   │
│   │                                                                                  │   │
│   │   Code:                                                                          │   │
│   │   ┌──────────────────────────────────────────────────────────────────────────┐  │   │
│   │   │ @ray.remote(num_cpus=0.25, max_retries=3, retry_exceptions=True)          │  │   │
│   │   │ def tokenize_article(article, ...):                                       │  │   │
│   │   │     # Ray automatically retries on any exception                          │  │   │
│   │   │     # Failed tasks are rescheduled on any available worker                │  │   │
│   │   └──────────────────────────────────────────────────────────────────────────┘  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ POD / NODE FAILURES                                                              │   │
│   │                                                                                  │   │
│   │   Worker pod crash      Kubernetes liveness    K8s restarts pod; Ray reschedules │   │
│   │   Worker pod eviction   K8s eviction          Ray reschedules pending tasks      │   │
│   │   Node failure          K8s node monitoring   Pods rescheduled to other nodes    │   │
│   │                                                                                  │   │
│   │   Flow:                                                                          │   │
│   │   ┌──────────────────────────────────────────────────────────────────────────┐  │   │
│   │   │                                                                           │  │   │
│   │   │   Worker Pod Crash                                                        │  │   │
│   │   │         │                                                                 │  │   │
│   │   │         ▼                                                                 │  │   │
│   │   │   Ray detects worker disconnect                                           │  │   │
│   │   │         │                                                                 │  │   │
│   │   │         ▼                                                                 │  │   │
│   │   │   In-flight tasks on that worker → FAILED                                 │  │   │
│   │   │         │                                                                 │  │   │
│   │   │         ▼                                                                 │  │   │
│   │   │   Ray reschedules failed tasks to healthy workers                         │  │   │
│   │   │         │                                                                 │  │   │
│   │   │         ▼                                                                 │  │   │
│   │   │   Kubernetes restarts crashed pod (separate from Ray retry)               │  │   │
│   │   │                                                                           │  │   │
│   │   └──────────────────────────────────────────────────────────────────────────┘  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ HEAD POD FAILURE (Most Critical)                                                 │   │
│   │                                                                                  │   │
│   │   Head pod crash        Kubernetes/Ray        Job fails; resume from checkpoint  │   │
│   │   Head pod OOM          K8s OOMKilled         Job fails; resume from checkpoint  │   │
│   │                                                                                  │   │
│   │   Impact:                                                                        │   │
│   │   • Driver process terminates                                                   │   │
│   │   • In-flight tasks in Ray object store are lost                                │   │
│   │   • Partial results on PVC survive                                              │   │
│   │   • Checkpoint file enables resume                                              │   │
│   │                                                                                  │   │
│   │   Recovery:                                                                      │   │
│   │   ┌──────────────────────────────────────────────────────────────────────────┐  │   │
│   │   │ # Delete failed job                                                       │  │   │
│   │   │ kubectl delete rayjob wiki-cirrus -n wiki-tokenizer                       │  │   │
│   │   │                                                                           │  │   │
│   │   │ # Resubmit - automatically resumes from checkpoint                        │  │   │
│   │   │ kubectl apply -n wiki-tokenizer -f k8s/rayjob-cirrus.yaml                 │  │   │
│   │   └──────────────────────────────────────────────────────────────────────────┘  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ DUMP DOWNLOAD FAILURES                                                           │   │
│   │                                                                                  │   │
│   │   Download interrupted  Partial file on PVC   Resume with curl -C -              │   │
│   │   Corrupted download    gzip -t fails         Delete and retry (up to 3x)        │   │
│   │   Network timeout       curl error            Retry with exponential backoff     │   │
│   │                                                                                  │   │
│   │   Mechanism:                                                                     │   │
│   │   ┌──────────────────────────────────────────────────────────────────────────┐  │   │
│   │   │ 1. Check for existing file + .complete marker → skip (instant)            │  │   │
│   │   │ 2. If file exists without marker:                                         │  │   │
│   │   │    a. Quick size check via HTTP HEAD (~1 sec)                             │  │   │
│   │   │       • Size mismatch → resume download (skip slow verification)          │  │   │
│   │   │    b. Size matches → full gzip -t verification (~3-7 min for 40GB)        │  │   │
│   │   │       • Valid → create marker, use file                                   │  │   │
│   │   │       • Invalid → delete and re-download                                  │  │   │
│   │   │ 3. After download: verify with gzip -t                                    │  │   │
│   │   │ 4. If valid: create .complete marker                                      │  │   │
│   │   │ 5. If invalid: delete and retry (up to 3x)                                │  │   │
│   │   │                                                                           │  │   │
│   │   │ Quick size check avoids slow gzip verification for partial downloads!    │  │   │
│   │   └──────────────────────────────────────────────────────────────────────────┘  │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │ DATA PERSISTENCE                                                                 │   │
│   │                                                                                  │   │
│   │   Pod restart           PVC mounted           Data survives (not ephemeral)      │   │
│   │   Job TTL cleanup       PVC independent       Data survives (1hr TTL)            │   │
│   │   Cluster restart       PVC on NFS/EFS        Data survives                      │   │
│   │   PVC deletion          Manual action         DATA LOST (only way to lose data)  │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Checkpointing System

The checkpoint system enables resumable jobs:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           CHECKPOINT MECHANISM                                           │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   CHECKPOINT FILE FORMAT                                                                 │
│   ──────────────────────                                                                 │
│                                                                                          │
│   /output/checkpoint.json:                                                               │
│   {                                                                                      │
│     "completed": ["12345", "12346", "12347", ...],  // page_ids or titles               │
│     "count": 50000,                                                                      │
│     "timestamp": 1700000000.123                                                          │
│   }                                                                                      │
│                                                                                          │
│   CHECKPOINT FLOW                                                                        │
│   ───────────────                                                                        │
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                                                                  │   │
│   │   JOB START                                                                      │   │
│   │       │                                                                          │   │
│   │       ▼                                                                          │   │
│   │   ┌──────────────────────────────────────┐                                       │   │
│   │   │ Load checkpoint                      │                                       │   │
│   │   │ already_completed = load_checkpoint()│                                       │   │
│   │   └──────────────────────────────────────┘                                       │   │
│   │       │                                                                          │   │
│   │       ▼                                                                          │   │
│   │   ┌──────────────────────────────────────┐                                       │   │
│   │   │ Filter input                         │                                       │   │
│   │   │ articles = [a for a in stream        │                                       │   │
│   │   │             if a.page_id not in      │                                       │   │
│   │   │             already_completed]       │                                       │   │
│   │   └──────────────────────────────────────┘                                       │   │
│   │       │                                                                          │   │
│   │       ▼                                                                          │   │
│   │   PROCESSING LOOP                                                                │   │
│   │       │                                                                          │   │
│   │       ├───▶ Process article                                                      │   │
│   │       │         │                                                                │   │
│   │       │         ▼                                                                │   │
│   │       │     completed_items.append(page_id)                                      │   │
│   │       │         │                                                                │   │
│   │       │         ▼                                                                │   │
│   │       │     ┌──────────────────────────────────────┐                             │   │
│   │       │     │ Every 100 completions:               │                             │   │
│   │       │     │ save_checkpoint(checkpoint_path,     │                             │   │
│   │       │     │                 completed_items)     │                             │   │
│   │       │     └──────────────────────────────────────┘                             │   │
│   │       │                                                                          │   │
│   │       └───◀ (loop)                                                               │   │
│   │                                                                                  │   │
│   │   JOB END                                                                        │   │
│   │       │                                                                          │   │
│   │       ▼                                                                          │   │
│   │   ┌──────────────────────────────────────┐                                       │   │
│   │   │ Final checkpoint save                │                                       │   │
│   │   │ save_checkpoint(checkpoint_path,     │                                       │   │
│   │   │                 completed_items)     │                                       │   │
│   │   └──────────────────────────────────────┘                                       │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│   ATOMIC WRITE                                                                           │
│   ────────────                                                                           │
│                                                                                          │
│   Checkpoint writes use atomic file operations to prevent corruption:                    │
│                                                                                          │
│   ┌──────────────────────────────────────────────────────────────────────────────────┐  │
│   │ tmp = checkpoint_path.with_suffix(".tmp")                                         │  │
│   │ tmp.write_text(json.dumps(data))  # Write to temp file                           │  │
│   │ tmp.replace(checkpoint_path)       # Atomic rename                               │  │
│   └──────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                          │
│   If crash during write: .tmp file is incomplete, original .json is intact              │
│   If crash after rename: new checkpoint is complete                                      │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Idempotent Task Execution

Tasks check for existing output before processing, making them safe to re-run:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           IDEMPOTENT EXECUTION                                           │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   FIRST RUN                                     RESTART (after crash)                    │
│   ─────────                                     ────────────────────                     │
│                                                                                          │
│   Article: "Machine learning"                   Article: "Machine learning"              │
│       │                                             │                                    │
│       ▼                                             ▼                                    │
│   ┌────────────────────────┐                   ┌────────────────────────┐               │
│   │ Check: output exists?  │                   │ Check: output exists?  │               │
│   │ /output/tokens/        │                   │ /output/tokens/        │               │
│   │ Machine_learning_*.json│                   │ Machine_learning_*.json│               │
│   └────────────────────────┘                   └────────────────────────┘               │
│       │                                             │                                    │
│       │ NO                                          │ YES                                │
│       ▼                                             ▼                                    │
│   ┌────────────────────────┐                   ┌────────────────────────┐               │
│   │ Tokenize article       │                   │ Return immediately:    │               │
│   │ Write output file      │                   │ {"status": "skipped",  │               │
│   │ Return {"status": "ok"}│                   │  "reason":             │               │
│   └────────────────────────┘                   │  "output_exists"}      │               │
│                                                 └────────────────────────┘               │
│                                                                                          │
│   BENEFITS:                                                                              │
│   • Safe to restart jobs at any point                                                   │
│   • No duplicate processing                                                             │
│   • Progress is never lost (output files = progress)                                    │
│   • Works even without checkpoint file (file-based idempotency)                         │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Graceful Error Handling

Failed tasks don't crash the job; they're logged and the job continues:

```python
try:
    token_ids = tokenize_text(text, encoding_name)
    return {"title": title, "status": "ok", "token_count": len(token_ids)}
except Exception as exc:
    # Don't crash - return error result so job can continue
    return {"title": title, "status": "error", "error": str(exc)}
```

Final summary shows success/failure breakdown:
```json
{
  "pages": 6000000,
  "ok": 5999850,
  "skipped": 100,
  "errors": 50,
  "total_tokens": 15000000000
}
```

---

## Strategies for Improvement

### Tier 1: Quick Wins (Hours of Work)

#### 1.1 Parallel Dump Streaming

**Current:** Single-threaded reading of gzipped dump files.

**Improvement:** Use Ray Data or multiprocessing to read multiple dump files in parallel.

```python
# Current: Sequential
for article in stream_cirrus_dir("/dumps"):
    process(article)

# Improved: Parallel with Ray Data
ds = ray.data.read_json("/dumps/*.json.gz", parallelism=8)
ds.map(tokenize_article).write_json("/output/tokens/")
```

**Expected Impact:** 2-4x faster dump ingestion.

#### 1.2 Batch Tokenization

**Current:** One Ray task per article.

**Improvement:** Batch multiple articles per task to reduce Ray overhead.

```python
@ray.remote
def tokenize_batch(articles: List[Dict], encoding_name: str):
    encoder = tiktoken.get_encoding(encoding_name)  # Load once
    return [{"tokens": encoder.encode(a["text"])} for a in articles]
```

**Expected Impact:** 30-50% reduction in task scheduling overhead.

#### 1.3 Pre-download Dumps as Init Job

**Current:** Init container downloads dump on first run of RayJob.

**Improvement:** Separate Kubernetes Job for downloading, run once per cluster.

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: download-wiki-dumps
spec:
  template:
    spec:
      containers:
      - name: downloader
        image: curlimages/curl
        command: ["curl", "-L", "-o", "/dumps/enwiki.json.gz", "..."]
```

**Expected Impact:** Cleaner separation; RayJobs start immediately.

### Tier 2: Medium Effort (Days of Work)

#### 2.1 Sharded Jobs

**Current:** Single RayJob processes entire 40GB dump.

**Improvement:** Split into multiple jobs, one per dump file chunk.

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           SHARDED JOB ARCHITECTURE                                       │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   CURRENT: MONOLITHIC                         IMPROVED: SHARDED                          │
│   ───────────────────                         ─────────────────                          │
│                                                                                          │
│   ┌──────────────────────┐                    ┌──────────────────────┐                  │
│   │     Single RayJob    │                    │    Job Controller    │                  │
│   │                      │                    │    (CronJob/Script)  │                  │
│   │   Process 6M pages   │                    └──────────┬───────────┘                  │
│   │   in one job         │                               │                              │
│   │                      │                    ┌──────────┼──────────┐                   │
│   │   Failure = restart  │                    ▼          ▼          ▼                   │
│   │   from checkpoint    │               ┌────────┐ ┌────────┐ ┌────────┐              │
│   │                      │               │ Shard 1│ │ Shard 2│ │ Shard 3│              │
│   └──────────────────────┘               │ 500K   │ │ 500K   │ │ 500K   │              │
│                                          │ pages  │ │ pages  │ │ pages  │              │
│                                          └────────┘ └────────┘ └────────┘              │
│                                                                                          │
│   BENEFITS:                                                                              │
│   • Smaller blast radius on failure                                                     │
│   • Independent progress tracking per shard                                             │
│   • Can run shards on different clusters                                                │
│   • Easier to retry individual shards                                                   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

#### 2.2 External Manifest with Object Storage

**Current:** Checkpoint stored on PVC.

**Improvement:** Store job manifest in S3/GCS for external visibility.

```json
{
  "job_id": "enwiki-20240101",
  "shards": [
    {"shard_id": "001", "file": "enwiki-part1.json.gz", "status": "completed", "pages": 500000},
    {"shard_id": "002", "file": "enwiki-part2.json.gz", "status": "running", "pages": 350000},
    {"shard_id": "003", "file": "enwiki-part3.json.gz", "status": "pending", "pages": 0}
  ]
}
```

**Benefits:**
- External monitoring without kubectl access
- Multiple job submitters can coordinate
- Easy integration with dashboards

#### 2.3 Streaming Embeddings

**Current:** Tokenization completes before embedding starts.

**Improvement:** Start embedding as soon as tokens are available.

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           STREAMING PIPELINE                                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   CURRENT: SEQUENTIAL                         IMPROVED: STREAMING                        │
│   ───────────────────                         ───────────────────                        │
│                                                                                          │
│   Time ──────────────────▶                    Time ──────────────────▶                  │
│                                                                                          │
│   ┌────────────────────────┐                  ┌──────────┬──────────┐                   │
│   │     Tokenization       │                  │ Tokenize │ Tokenize │                   │
│   │     (all articles)     │                  │ batch 1  │ batch 2  │ ...              │
│   └────────────────────────┘                  └────┬─────┴────┬─────┘                   │
│             │                                      │          │                          │
│             ▼                                      ▼          ▼                          │
│   ┌────────────────────────┐                  ┌──────────┬──────────┐                   │
│   │      Embedding         │                  │ Embed    │ Embed    │                   │
│   │     (all articles)     │                  │ batch 1  │ batch 2  │ ...              │
│   └────────────────────────┘                  └──────────┴──────────┘                   │
│                                                                                          │
│   Total time: T_tok + T_emb                   Total time: max(T_tok, T_emb)             │
│                                               (overlapping execution)                    │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Tier 3: Production Hardening (Weeks of Work)

#### 3.1 Job Monitor Service

A separate Deployment that watches RayJob status and handles failures:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: wiki-job-monitor
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: monitor
        image: tokenize-wiki:prod
        command: ["python", "-m", "ray_app.job_monitor"]
        env:
        - name: SLACK_WEBHOOK_URL
          valueFrom:
            secretKeyRef:
              name: alerting-secrets
              key: slack-webhook
```

**Responsibilities:**
- Poll RayJob status every minute
- Resubmit failed jobs automatically
- Send Slack/PagerDuty alerts on repeated failures
- Update external manifest with progress

#### 3.2 Head Pod High Availability

Ray supports GCS fault tolerance with external Redis:

```yaml
rayStartParams:
  redis-address: "redis-ha:6379"
  redis-password: "${REDIS_PASSWORD}"
```

**Benefits:**
- Head pod can restart without losing cluster state
- In-flight tasks survive head pod restart
- Requires Redis HA deployment (adds complexity)

#### 3.3 Observability Stack

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           OBSERVABILITY ARCHITECTURE                                     │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                           METRICS (Prometheus)                                   │   │
│   │                                                                                  │   │
│   │   Ray exports metrics at /metrics:                                              │   │
│   │   • ray_tasks_running                                                           │   │
│   │   • ray_tasks_completed_total                                                   │   │
│   │   • ray_object_store_memory_used_bytes                                          │   │
│   │   • ray_node_cpu_utilization                                                    │   │
│   │                                                                                  │   │
│   │   Custom metrics:                                                               │   │
│   │   • wiki_articles_processed_total                                               │   │
│   │   • wiki_tokenization_duration_seconds                                          │   │
│   │   • wiki_embedding_duration_seconds                                             │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                                │
│                                         ▼                                                │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                           DASHBOARDS (Grafana)                                   │   │
│   │                                                                                  │   │
│   │   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                 │   │
│   │   │ Throughput      │  │ Error Rate      │  │ Resource Usage  │                 │   │
│   │   │ articles/sec    │  │ % failed tasks  │  │ CPU/GPU/Memory  │                 │   │
│   │   └─────────────────┘  └─────────────────┘  └─────────────────┘                 │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                                │
│                                         ▼                                                │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                           ALERTING                                               │   │
│   │                                                                                  │   │
│   │   Alert: HighErrorRate                                                          │   │
│   │   expr: rate(wiki_tasks_failed_total[5m]) > 0.1                                 │   │
│   │   for: 5m                                                                       │   │
│   │   annotations:                                                                  │   │
│   │     summary: "High task failure rate"                                           │   │
│   │                                                                                  │   │
│   │   Alert: JobStuck                                                               │   │
│   │   expr: increase(wiki_articles_processed_total[10m]) == 0                       │   │
│   │   for: 10m                                                                      │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                         │                                                │
│                                         ▼                                                │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│   │                           LOGGING (Loki/ELK)                                     │   │
│   │                                                                                  │   │
│   │   Centralized logs from all pods:                                               │   │
│   │   • Driver progress logs                                                        │   │
│   │   • Worker task logs                                                            │   │
│   │   • Error stack traces                                                          │   │
│   │   • Checkpoint saves                                                            │   │
│   │                                                                                  │   │
│   └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

#### 3.4 Vector Database Integration

Store embeddings in a vector database for semantic search:

```python
# After generating embeddings
import chromadb

client = chromadb.HttpClient(host="chromadb-svc", port=8000)
collection = client.get_or_create_collection("wikipedia")

for article in articles:
    embedding = np.load(article["embedding_file"])
    collection.add(
        ids=[article["page_id"]],
        embeddings=[embedding.tolist()],
        metadatas=[{"title": article["title"]}]
    )
```

---

## Quick Reference

### Key Files

| File | Purpose |
|------|---------|
| `ray_app/tokenize_wiki.py` | Main tokenization job (supports API + CirrusSearch modes) |
| `ray_app/cirrus_reader.py` | CirrusSearch dump streaming reader |
| `ray_app/embed_wiki.py` | GPU-accelerated embedding generation |
| `ray_app/pipeline.py` | Combined tokenization + embedding pipeline |
| `k8s/rayjob-cirrus.yaml` | RayJob for CirrusSearch processing |
| `k8s/rayjob-embed.yaml` | RayJob for embedding generation |
| `k8s/rayjob-pipeline.yaml` | Combined pipeline RayJob |
| `k8s/pvc.yaml` | Output storage PVC |
| `k8s/pvc-dumps.yaml` | Dump storage PVC |

### Common Commands

```bash
# Setup
kubectl create ns wiki-tokenizer
./scripts/install_kuberay.sh
kubectl apply -n wiki-tokenizer -f k8s/pvc.yaml
kubectl apply -n wiki-tokenizer -f k8s/pvc-dumps.yaml

# Run CirrusSearch tokenization
kubectl apply -n wiki-tokenizer -f k8s/rayjob-cirrus.yaml

# Run embedding (after tokenization)
kubectl apply -n wiki-tokenizer -f k8s/rayjob-embed.yaml

# Monitor
kubectl get rayjob -n wiki-tokenizer -w
kubectl logs -n wiki-tokenizer -l ray.io/node-type=head -f

# Ray Dashboard
kubectl port-forward -n wiki-tokenizer svc/wiki-cirrus-head-svc 8265:8265
# Open http://localhost:8265

# Resume after failure
kubectl delete rayjob wiki-cirrus -n wiki-tokenizer
kubectl apply -n wiki-tokenizer -f k8s/rayjob-cirrus.yaml  # Resumes from checkpoint

# Get results
kubectl exec -it -n wiki-tokenizer \
  $(kubectl get pod -n wiki-tokenizer -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}') \
  -- ls -la /output/
```

### Performance Benchmarks

| Metric | Live API Mode | CirrusSearch Mode |
|--------|---------------|-------------------|
| Throughput | ~1-2 pages/sec | ~100-500 pages/sec |
| Rate limits | Yes (429 errors) | None |
| Network I/O | High (per-page fetch) | Low (local read) |
| Storage needed | Minimal | ~50GB for enwiki |
| Reproducibility | No (content changes) | Yes (fixed snapshot) |

### Resource Recommendations

| Workload | Head Pod | CPU Workers | GPU Workers |
|----------|----------|-------------|-------------|
| Small (<1K pages) | 2 CPU, 4Gi | 1 replica, 2 CPU each | 1 replica |
| Medium (1K-100K) | 4 CPU, 8Gi | 4 replicas, 2 CPU each | 2 replicas |
| Large (>100K) | 4 CPU, 16Gi | 8 replicas, 4 CPU each | 4 replicas |

---

## Summary

This architecture provides a robust, scalable pipeline for processing Wikipedia at scale:

1. **Two-stage pipeline** separates CPU-bound tokenization from GPU-accelerated embedding
2. **CirrusSearch mode** enables bulk processing without API rate limits
3. **Kubernetes + Ray** provides distributed execution with automatic scaling
4. **Comprehensive failure handling** through retries, checkpoints, and idempotent tasks
5. **Persistent storage** ensures data survives pod restarts and job failures

The system can process millions of Wikipedia articles, producing token sequences and vector embeddings suitable for training language models or building semantic search applications.
