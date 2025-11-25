#!/usr/bin/env python3
"""
GPU-accelerated embedding generation for Wikipedia articles using sentence-transformers.

This script reads tokenized Wikipedia articles and generates embeddings using
the MiniLM model on NVIDIA GPUs via Ray distributed computing.

Designed for scale:
- Streaming file iteration (doesn't load all articles into memory)
- Streaming index writes (doesn't accumulate all results)
- Works with CPU-only head nodes (GPU check happens on workers)

Usage:
    python -m ray_app.embed_wiki \
        --input-dir /output/tokens \
        --output-dir /output/embeddings \
        --model all-MiniLM-L6-v2 \
        --concurrency 2
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set

import numpy as np
import ray

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def _slug_filename(title: str) -> str:
    """Create a safe filename from a Wikipedia title."""
    import hashlib
    safe = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)
    safe = safe[:50]
    h = hashlib.md5(title.encode()).hexdigest()[:8]
    return f"{safe}_{h}"


@ray.remote(num_gpus=1, max_retries=2, retry_exceptions=True)
def generate_embeddings_batch(
    articles: List[Dict],
    model_name: str,
    output_dir: str,
) -> List[Dict]:
    """
    Generate embeddings for a batch of articles using GPU.
    More efficient than individual calls due to model loading overhead.

    Args:
        articles: List of dicts with 'title' and 'text' keys
        model_name: Sentence-transformers model name
        output_dir: Directory to save embeddings

    Returns:
        List of result dicts
    """
    from sentence_transformers import SentenceTransformer

    # Load model once for the batch on GPU
    model = SentenceTransformer(model_name, device="cuda")
    
    results = []
    for article in articles:
        title = article["title"]
        text = article["text"]
        start = time.perf_counter()
        output_path = Path(output_dir) / f"{_slug_filename(title)}.npy"
        
        # Skip if already processed
        if output_path.exists():
            results.append({
                "title": title,
                "status": "skipped",
                "reason": "output_exists",
                "embedding_file": str(output_path),
            })
            continue
        
        try:
            embedding = model.encode(text, convert_to_numpy=True, show_progress_bar=False)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            np.save(output_path, embedding)
            
            duration = time.perf_counter() - start
            results.append({
                "title": title,
                "status": "ok",
                "embedding_dim": len(embedding),
                "embedding_file": str(output_path),
                "duration_sec": round(duration, 3),
            })
        except Exception as exc:
            duration = time.perf_counter() - start
            log.error(f"Error embedding '{title}': {exc}")
            results.append({
                "title": title,
                "status": "error",
                "error": str(exc),
                "duration_sec": round(duration, 3),
            })
    
    return results


def stream_articles_from_tokens(
    tokens_dir: Path,
    completed: Optional[Set[str]] = None,
) -> Iterator[Dict]:
    """
    Stream article texts from token JSON files (memory-efficient).
    
    Yields articles one at a time instead of loading all into memory.
    This is critical for processing millions of articles.
    
    Each token file should have format:
    {
        "title": "...",
        "text": "...",
        "tokens": [...],
        "token_count": N
    }
    """
    completed = completed or set()
    skipped_empty = 0
    skipped_no_tokens = 0
    skipped_missing_fields = 0
    skipped_malformed = 0
    skipped_checkpoint = 0
    yielded = 0
    
    for token_file in tokens_dir.glob("*.json"):
        try:
            with open(token_file) as f:
                data = json.load(f)
            
            # Check required fields
            if "title" not in data or "text" not in data:
                skipped_missing_fields += 1
                continue
            
            title = data["title"]
            text = data["text"]
            token_count = data.get("token_count", 0)
            
            # Skip if already completed (from checkpoint)
            if title in completed:
                skipped_checkpoint += 1
                continue
            
            # Validate text is non-empty
            if not text or not text.strip():
                skipped_empty += 1
                continue
            
            # Validate tokenization produced tokens
            if token_count == 0:
                skipped_no_tokens += 1
                continue
            
            yielded += 1
            yield {
                "title": title,
                "text": text,
                "token_count": token_count,
                "source_file": str(token_file),
            }
            
        except json.JSONDecodeError:
            skipped_malformed += 1
        except Exception:
            skipped_malformed += 1
    
    # Log summary at end
    total_skipped = (skipped_empty + skipped_no_tokens + 
                     skipped_missing_fields + skipped_malformed)
    if total_skipped > 0 or skipped_checkpoint > 0:
        log.info(f"Streaming complete: yielded={yielded}, "
                 f"from_checkpoint={skipped_checkpoint}, "
                 f"skipped={total_skipped} (empty={skipped_empty}, "
                 f"no_tokens={skipped_no_tokens}, malformed={skipped_malformed})")


def count_token_files(tokens_dir: Path) -> int:
    """Count token files without loading them (for progress estimation)."""
    return sum(1 for _ in tokens_dir.glob("*.json"))


def load_checkpoint(checkpoint_path: Optional[Path]) -> Set[str]:
    """Load set of already-processed titles from checkpoint."""
    if not checkpoint_path or not checkpoint_path.exists():
        return set()
    try:
        with open(checkpoint_path) as f:
            data = json.load(f)
        completed = set(data.get("completed", []))
        log.info(f"Loaded checkpoint with {len(completed)} completed titles")
        return completed
    except Exception:
        return set()


def save_checkpoint(checkpoint_path: Optional[Path], completed_titles: List[str]):
    """Save checkpoint with completed titles."""
    if not checkpoint_path:
        return
    data = {
        "completed": completed_titles,
        "count": len(completed_titles),
        "timestamp": time.time(),
    }
    tmp = checkpoint_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data))
    tmp.replace(checkpoint_path)


def main():
    parser = argparse.ArgumentParser(description="Generate embeddings for Wikipedia articles")
    parser.add_argument("--input-dir", required=True, help="Directory with token JSON files")
    parser.add_argument("--output-dir", required=True, help="Directory to save embeddings")
    parser.add_argument("--index-file", help="Output JSONL index file (default: <output-dir>/embedding_index.jsonl)")
    parser.add_argument("--checkpoint", help="Checkpoint file for resume support")
    parser.add_argument("--model", default="all-MiniLM-L6-v2", help="Sentence-transformers model name")
    parser.add_argument("--batch-size", type=int, default=10, help="Articles per batch task")
    parser.add_argument("--concurrency", type=int, default=2, help="Max concurrent GPU tasks")
    parser.add_argument("--max-articles", type=int, help="Limit number of articles to process")
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    index_file = Path(args.index_file) if args.index_file else output_dir / "embedding_index.jsonl"
    checkpoint_path = Path(args.checkpoint) if args.checkpoint else None
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Count files for progress estimation (fast, doesn't load content)
    total_files = count_token_files(input_dir)
    log.info(f"Found {total_files} token files in {input_dir}")
    
    if total_files == 0:
        log.error("No token files found! Check --input-dir path.")
        sys.exit(1)
    
    # Load checkpoint
    completed = load_checkpoint(checkpoint_path)
    completed_titles = list(completed)
    
    # Initialize Ray (connect to existing cluster)
    log.info("Initializing Ray...")
    ray.init(address="auto", ignore_reinit_error=True)
    
    # Log cluster resources (don't fail if no GPU on head - workers may have GPUs)
    cluster_resources = ray.cluster_resources()
    log.info(f"Cluster resources: {cluster_resources}")
    
    num_gpus = cluster_resources.get("GPU", 0)
    if num_gpus == 0:
        log.warning("No GPUs detected in Ray cluster! Embedding tasks will queue until GPU workers join.")
        log.warning("If this persists, check that GPU workers are configured correctly.")
    else:
        log.info(f"Cluster has {num_gpus} GPU(s) available")
    
    log.info(f"Model: {args.model}")
    log.info(f"Output: {output_dir}")
    log.info(f"Batch size: {args.batch_size}, Concurrency: {args.concurrency}")
    
    # Streaming processing with batching
    pending_refs: List[ray.ObjectRef] = []
    current_batch: List[Dict] = []
    
    submitted = 0
    ok_count = 0
    skip_count = 0
    err_count = 0
    checkpoint_interval = 100
    
    start_time = time.perf_counter()
    
    # Open index file for streaming writes (append mode for resume)
    index_file.parent.mkdir(parents=True, exist_ok=True)
    
    def process_completed_tasks():
        """Process any completed tasks and update counters."""
        nonlocal ok_count, skip_count, err_count
        
        if not pending_refs:
            return
        
        # Check for completed tasks (non-blocking with timeout=0)
        done_refs, remaining = ray.wait(pending_refs, num_returns=len(pending_refs), timeout=0)
        pending_refs[:] = remaining
        
        for ref in done_refs:
            try:
                batch_results = ray.get(ref)
                # Stream results to index file
                with open(index_file, "a") as f:
                    for r in batch_results:
                        f.write(json.dumps(r) + "\n")
                        if r["status"] == "ok":
                            ok_count += 1
                            completed_titles.append(r["title"])
                        elif r["status"] == "skipped":
                            skip_count += 1
                            completed_titles.append(r["title"])
                        else:
                            err_count += 1
                
                # Progress log
                total_done = ok_count + skip_count + err_count
                log.info(f"[{total_done}/{submitted}] ok={ok_count} skip={skip_count} err={err_count}")
                
                # Save checkpoint periodically
                if checkpoint_path and total_done % checkpoint_interval == 0:
                    save_checkpoint(checkpoint_path, completed_titles)
                    
            except Exception as e:
                log.error(f"Batch task failed: {e}")
                err_count += args.batch_size
    
    # Stream articles and submit batches
    article_stream = stream_articles_from_tokens(input_dir, completed)
    
    for article in article_stream:
        current_batch.append(article)
        
        # Submit batch when full
        if len(current_batch) >= args.batch_size:
            # Wait if at concurrency limit
            while len(pending_refs) >= args.concurrency:
                done_refs, pending_refs[:] = ray.wait(pending_refs, num_returns=1, timeout=60)
                for ref in done_refs:
                    try:
                        batch_results = ray.get(ref)
                        with open(index_file, "a") as f:
                            for r in batch_results:
                                f.write(json.dumps(r) + "\n")
                                if r["status"] == "ok":
                                    ok_count += 1
                                    completed_titles.append(r["title"])
                                elif r["status"] == "skipped":
                                    skip_count += 1
                                    completed_titles.append(r["title"])
                                else:
                                    err_count += 1
                        total_done = ok_count + skip_count + err_count
                        log.info(f"[{total_done}/{submitted}] ok={ok_count} skip={skip_count} err={err_count}")
                        if checkpoint_path and total_done % checkpoint_interval == 0:
                            save_checkpoint(checkpoint_path, completed_titles)
                    except Exception as e:
                        log.error(f"Batch task failed: {e}")
                        err_count += args.batch_size
            
            # Submit the batch
            ref = generate_embeddings_batch.remote(current_batch, args.model, str(output_dir))
            pending_refs.append(ref)
            submitted += len(current_batch)
            current_batch = []
        
        # Check max articles limit
        if args.max_articles and submitted >= args.max_articles:
            log.info(f"Reached max_articles limit ({args.max_articles})")
            break
    
    # Submit final partial batch
    if current_batch:
        ref = generate_embeddings_batch.remote(current_batch, args.model, str(output_dir))
        pending_refs.append(ref)
        submitted += len(current_batch)
    
    log.info(f"Submitted {submitted} articles, waiting for {len(pending_refs)} pending batches...")
    
    # Drain remaining tasks
    while pending_refs:
        done_refs, pending_refs = ray.wait(pending_refs, num_returns=1, timeout=300)
        for ref in done_refs:
            try:
                batch_results = ray.get(ref)
                with open(index_file, "a") as f:
                    for r in batch_results:
                        f.write(json.dumps(r) + "\n")
                        if r["status"] == "ok":
                            ok_count += 1
                            completed_titles.append(r["title"])
                        elif r["status"] == "skipped":
                            skip_count += 1
                            completed_titles.append(r["title"])
                        else:
                            err_count += 1
                total_done = ok_count + skip_count + err_count
                log.info(f"[{total_done}/{submitted}] ok={ok_count} skip={skip_count} err={err_count}")
                if checkpoint_path and total_done % checkpoint_interval == 0:
                    save_checkpoint(checkpoint_path, completed_titles)
            except Exception as e:
                log.error(f"Batch task failed: {e}")
                err_count += args.batch_size
    
    elapsed = time.perf_counter() - start_time
    
    # Final checkpoint save
    if checkpoint_path:
        save_checkpoint(checkpoint_path, completed_titles)
    
    # Summary
    total_processed = ok_count + skip_count + err_count
    log.info("")
    log.info("=" * 50)
    log.info("EMBEDDING GENERATION COMPLETE")
    log.info("=" * 50)
    log.info(f"Total processed: {total_processed}")
    log.info(f"  OK:      {ok_count}")
    log.info(f"  Skipped: {skip_count}")
    log.info(f"  Errors:  {err_count}")
    if total_processed > 0:
        log.info(f"Time: {elapsed:.1f}s ({total_processed/elapsed:.2f} articles/sec)")
    log.info(f"Output: {output_dir}")
    log.info(f"Index:  {index_file}")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
