#!/usr/bin/env python3
"""
End-to-end Wikipedia pipeline: Tokenization → Embedding

This script runs both stages in sequence within a single Ray job:
1. Tokenize Wikipedia articles (CPU-bound, uses Wikipedia API)
2. Generate embeddings for tokenized articles (GPU-accelerated)

Usage:
    python -m ray_app.pipeline \
        --pages-file /data/sample_pages.txt \
        --output-dir /output \
        --model all-MiniLM-L6-v2 \
        --concurrency 4
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def run_tokenization(args) -> bool:
    """Run the tokenization stage."""
    from ray_app.tokenize_wiki import main as tokenize_main

    log.info("=" * 60)
    log.info("STAGE 1: TOKENIZATION")
    log.info("=" * 60)

    tokenize_args = [
        "--output", str(args.output_dir / "token_counts.jsonl"),
        "--tokens-dir", str(args.output_dir / "tokens"),
        "--checkpoint", str(args.output_dir / "tokenize_checkpoint.json"),
        "--concurrency", str(args.tokenize_concurrency),
    ]

    if args.pages_file:
        tokenize_args.extend(["--pages-file", args.pages_file])
    if args.pages:
        tokenize_args.extend(["--pages", args.pages])
    if args.max_pages:
        tokenize_args.extend(["--max-pages", str(args.max_pages)])

    log.info(f"Tokenization args: {tokenize_args}")

    try:
        exit_code = tokenize_main(tokenize_args)
        if exit_code != 0:
            log.warning(f"Tokenization had errors (exit code {exit_code})")
    except Exception as e:
        log.warning(f"Tokenization exception: {e}")

    # Check if any tokens were actually created
    tokens_dir = args.output_dir / "tokens"
    token_files = list(tokens_dir.glob("*.json")) if tokens_dir.exists() else []

    if not token_files:
        log.error("No token files created - cannot proceed to embedding")
        return False

    log.info(f"Tokenization produced {len(token_files)} token files")
    return True


def run_embedding(args) -> bool:
    """Run the embedding stage."""
    from ray_app.embed_wiki import main as embed_main
    
    log.info("")
    log.info("=" * 60)
    log.info("STAGE 2: EMBEDDING GENERATION")
    log.info("=" * 60)
    
    # Check if tokens exist
    tokens_dir = args.output_dir / "tokens"
    token_files = list(tokens_dir.glob("*.json"))
    if not token_files:
        log.error(f"No token files found in {tokens_dir}!")
        return False
    
    log.info(f"Found {len(token_files)} token files to process")
    
    # Build embedding args - embed_wiki.main() uses argparse internally
    # We need to simulate command line args
    embed_argv = [
        "--input-dir", str(tokens_dir),
        "--output-dir", str(args.output_dir / "embeddings"),
        "--checkpoint", str(args.output_dir / "embed_checkpoint.json"),
        "--model", args.model,
        "--batch-size", str(args.embed_batch_size),
        "--concurrency", str(args.embed_concurrency),
    ]
    
    log.info(f"Embedding args: {embed_argv}")
    
    # Temporarily replace sys.argv for embed_wiki.main()
    old_argv = sys.argv
    sys.argv = ["embed_wiki"] + embed_argv
    
    try:
        embed_main()
        log.info("Embedding generation completed successfully!")
        return True
    except SystemExit as e:
        if e.code == 0:
            log.info("Embedding generation completed successfully!")
            return True
        log.error(f"Embedding failed with exit code {e.code}")
        return False
    except Exception as e:
        log.error(f"Embedding failed with exception: {e}")
        return False
    finally:
        sys.argv = old_argv


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end Wikipedia tokenization and embedding pipeline"
    )
    
    # Input options
    parser.add_argument("--pages", help="Comma-separated page titles")
    parser.add_argument("--pages-file", help="Path to file with newline-delimited titles")
    parser.add_argument("--max-pages", type=int, help="Limit number of pages processed")
    
    # Output options
    parser.add_argument(
        "--output-dir", 
        type=Path,
        default=Path("/output"),
        help="Base output directory (default: /output)"
    )
    
    # Tokenization options
    parser.add_argument(
        "--tokenize-concurrency", 
        type=int, 
        default=4,
        help="Concurrency for tokenization (default: 4)"
    )
    
    # Embedding options
    parser.add_argument(
        "--model",
        default="all-MiniLM-L6-v2",
        help="Sentence-transformers model (default: all-MiniLM-L6-v2)"
    )
    parser.add_argument(
        "--embed-batch-size",
        type=int,
        default=10,
        help="Articles per embedding batch (default: 10)"
    )
    parser.add_argument(
        "--embed-concurrency",
        type=int,
        default=2,
        help="Concurrent GPU embedding tasks (default: 2)"
    )
    
    # Stage control
    parser.add_argument(
        "--skip-tokenize",
        action="store_true",
        help="Skip tokenization (use existing tokens)"
    )
    parser.add_argument(
        "--skip-embed",
        action="store_true",
        help="Skip embedding generation"
    )
    
    args = parser.parse_args()
    
    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    log.info("=" * 60)
    log.info("WIKIPEDIA TOKENIZATION + EMBEDDING PIPELINE")
    log.info("=" * 60)
    log.info(f"Output directory: {args.output_dir}")
    log.info(f"Model: {args.model}")
    
    start_time = time.perf_counter()
    
    # Stage 1: Tokenization
    if not args.skip_tokenize:
        if not run_tokenization(args):
            log.error("Pipeline failed at tokenization stage")
            sys.exit(1)
    else:
        log.info("Skipping tokenization (--skip-tokenize)")
    
    # Stage 2: Embedding
    if not args.skip_embed:
        if not run_embedding(args):
            log.error("Pipeline failed at embedding stage")
            sys.exit(1)
    else:
        log.info("Skipping embedding (--skip-embed)")
    
    elapsed = time.perf_counter() - start_time
    
    # Final summary
    log.info("")
    log.info("=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)
    log.info(f"Total time: {elapsed:.1f}s")
    log.info(f"Tokens: {args.output_dir / 'tokens'}")
    log.info(f"Embeddings: {args.output_dir / 'embeddings'}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

