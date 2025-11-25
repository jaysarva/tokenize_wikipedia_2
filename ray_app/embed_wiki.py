#!/usr/bin/env python3
"""
GPU-accelerated embedding generation for Wikipedia articles using sentence-transformers.

This script reads tokenized Wikipedia articles and generates embeddings using
the MiniLM model on NVIDIA GPUs via Ray distributed computing.

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
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

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
def generate_embedding(
    title: str,
    text: str,
    model_name: str,
    output_dir: str,
) -> Dict:
    """
    Generate embedding for a single article using GPU.
    
    Args:
        title: Wikipedia article title
        text: Article text content
        model_name: Sentence-transformers model name
        output_dir: Directory to save embeddings
    
    Returns:
        Dict with title, status, embedding_dim, etc.
    """
    from sentence_transformers import SentenceTransformer
    
    start = time.perf_counter()
    output_path = Path(output_dir) / f"{_slug_filename(title)}.npy"
    
    # Skip if already processed (idempotent)
    if output_path.exists():
        return {
            "title": title,
            "status": "skipped",
            "reason": "output_exists",
            "embedding_file": str(output_path),
        }
    
    try:
        # Load model (cached after first load on this worker)
        model = SentenceTransformer(model_name, device="cuda")
        
        # Generate embedding
        embedding = model.encode(text, convert_to_numpy=True, show_progress_bar=False)
        
        # Save as numpy array
        output_path.parent.mkdir(parents=True, exist_ok=True)
        np.save(output_path, embedding)
        
        duration = time.perf_counter() - start
        
        return {
            "title": title,
            "status": "ok",
            "embedding_dim": len(embedding),
            "embedding_file": str(output_path),
            "duration_sec": round(duration, 3),
        }
        
    except Exception as exc:
        duration = time.perf_counter() - start
        log.error(f"Error embedding '{title}': {exc}")
        return {
            "title": title,
            "status": "error",
            "error": str(exc),
            "duration_sec": round(duration, 3),
        }


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
    
    # Load model once for the batch
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


def load_articles_from_tokens(tokens_dir: Path) -> List[Dict]:
    """
    Load article texts from token JSON files.
    
    Each token file should have format:
    {
        "title": "...",
        "text": "...",
        "token_ids": [...],
        ...
    }
    """
    articles = []
    for token_file in tokens_dir.glob("*.json"):
        try:
            with open(token_file) as f:
                data = json.load(f)
            if "title" in data and "text" in data:
                articles.append({
                    "title": data["title"],
                    "text": data["text"],
                    "source_file": str(token_file),
                })
        except Exception as e:
            log.warning(f"Failed to load {token_file}: {e}")
    return articles


def load_checkpoint(checkpoint_path: Path) -> Set[str]:
    """Load set of already-processed titles from checkpoint."""
    if not checkpoint_path.exists():
        return set()
    try:
        with open(checkpoint_path) as f:
            data = json.load(f)
        return set(data.get("completed", []))
    except Exception:
        return set()


def save_checkpoint(checkpoint_path: Path, completed_titles: List[str]):
    """Save checkpoint with completed titles."""
    data = {
        "completed": completed_titles,
        "count": len(completed_titles),
        "timestamp": time.time(),
    }
    tmp = checkpoint_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
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
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    index_file = Path(args.index_file) if args.index_file else output_dir / "embedding_index.jsonl"
    checkpoint_path = Path(args.checkpoint) if args.checkpoint else None
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load articles
    log.info(f"Loading articles from {input_dir}")
    articles = load_articles_from_tokens(input_dir)
    log.info(f"Found {len(articles)} articles with text")
    
    if not articles:
        log.error("No articles found! Check --input-dir path.")
        sys.exit(1)
    
    # Filter already completed from checkpoint
    if checkpoint_path:
        completed = load_checkpoint(checkpoint_path)
        original_count = len(articles)
        articles = [a for a in articles if a["title"] not in completed]
        log.info(f"Checkpoint: {len(completed)} already done, {len(articles)} remaining")
    
    # Initialize Ray
    log.info("Initializing Ray...")
    ray.init(address="auto", ignore_reinit_error=True)
    
    log.info(f"Model: {args.model}")
    log.info(f"Output: {output_dir}")
    log.info(f"Batch size: {args.batch_size}, Concurrency: {args.concurrency}")
    
    # Create batches
    batches = []
    for i in range(0, len(articles), args.batch_size):
        batches.append(articles[i:i + args.batch_size])
    
    log.info(f"Processing {len(articles)} articles in {len(batches)} batches")
    
    # Submit batch tasks with concurrency limit
    results = []
    completed_titles = list(completed) if checkpoint_path else []
    pending_refs = []
    batch_idx = 0
    ok_count = 0
    skip_count = 0
    err_count = 0
    
    start_time = time.perf_counter()
    
    while batch_idx < len(batches) or pending_refs:
        # Submit new tasks up to concurrency limit
        while len(pending_refs) < args.concurrency and batch_idx < len(batches):
            batch = batches[batch_idx]
            ref = generate_embeddings_batch.remote(batch, args.model, str(output_dir))
            pending_refs.append(ref)
            batch_idx += 1
        
        if not pending_refs:
            break
        
        # Wait for at least one task to complete
        done_refs, pending_refs = ray.wait(pending_refs, num_returns=1, timeout=300)
        
        for ref in done_refs:
            try:
                batch_results = ray.get(ref)
                for r in batch_results:
                    results.append(r)
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
                log.info(f"[{total_done}/{len(articles)}] ok={ok_count} skip={skip_count} err={err_count}")
                
                # Save checkpoint periodically
                if checkpoint_path and total_done % 50 == 0:
                    save_checkpoint(checkpoint_path, completed_titles)
                    
            except Exception as e:
                log.error(f"Batch task failed: {e}")
                err_count += args.batch_size  # Assume whole batch failed
    
    elapsed = time.perf_counter() - start_time
    
    # Final checkpoint save
    if checkpoint_path:
        save_checkpoint(checkpoint_path, completed_titles)
    
    # Write index file
    log.info(f"Writing index to {index_file}")
    with open(index_file, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    
    # Summary
    log.info("")
    log.info("=" * 50)
    log.info("EMBEDDING GENERATION COMPLETE")
    log.info("=" * 50)
    log.info(f"Total articles: {len(articles)}")
    log.info(f"  OK:      {ok_count}")
    log.info(f"  Skipped: {skip_count}")
    log.info(f"  Errors:  {err_count}")
    log.info(f"Time: {elapsed:.1f}s ({len(articles)/elapsed:.2f} articles/sec)")
    log.info(f"Output: {output_dir}")
    log.info(f"Index:  {index_file}")
    log.info("=" * 50)


if __name__ == "__main__":
    main()

