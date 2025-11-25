"""Ray job entrypoint to fetch and tokenize Wikipedia pages."""

import argparse
import json
import hashlib
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import ray
import requests
import tiktoken


WIKI_API_ENDPOINT = os.environ.get("WIKI_API_ENDPOINT", "https://en.wikipedia.org/w/api.php")
DEFAULT_ENCODING = os.environ.get("TIKTOKEN_ENCODING", "cl100k_base")
USER_AGENT = os.environ.get("WIKI_USER_AGENT", "tokenize-wikipedia/0.1 (contact: your-email@example.com)")


class WikipediaFetchError(Exception):
    """Raised when a page cannot be fetched from the API."""


def load_titles(args: argparse.Namespace) -> List[str]:
    if args.pages:
        return [title.strip() for title in args.pages.split(",") if title.strip()]
    if args.pages_file:
        return [line.strip() for line in Path(args.pages_file).read_text().splitlines() if line.strip()]
    # Default to the same sample list as toy_run.py
    return [
        "OpenAI",
        "Ray (distributed computing)",
        "Kubernetes",
        "Python (programming language)",
        "Machine learning",
        "Natural language processing",
        "Large language model",
        "Transformer (machine learning model)",
        "Tokenization",
        "Distributed computing",
    ]


def fetch_plaintext(title: str, endpoint: str, user_agent: str, max_attempts: int = 4) -> str:
    """Fetch Wikipedia plaintext with simple retries/backoff."""
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    backoff = 0.5
    for attempt in range(max_attempts):
        try:
            params = {
                "action": "query",
                "format": "json",
                "prop": "extracts",
                "explaintext": 1,
                "redirects": 1,
                "titles": title,
            }
            resp = session.get(endpoint, params=params, timeout=15)
            if resp.status_code >= 500 or resp.status_code == 429:
                raise WikipediaFetchError(f"HTTP {resp.status_code} for {title}")
            if not resp.ok:
                raise WikipediaFetchError(f"Request failed for {title}: {resp.status_code} {resp.text}")
            payload = resp.json()
            pages = payload.get("query", {}).get("pages", {})
            if not pages:
                raise WikipediaFetchError(f"No pages returned for {title}")
            first_page: Dict[str, Dict[str, str]] = next(iter(pages.values()))
            if first_page.get("missing"):
                normalized = first_page.get("title", title)
                raise WikipediaFetchError(f"Page missing for {title} (normalized: {normalized})")
            extract = first_page.get("extract")
            if not extract:
                normalized = first_page.get("title", title)
                raise WikipediaFetchError(f"No extract found for {title} (normalized: {normalized})")
            return extract
        except Exception:
            if attempt == max_attempts - 1:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)


def tokenize_text(text: str, encoding_name: str) -> List[int]:
    encoding = tiktoken.get_encoding(encoding_name)
    return encoding.encode(text)


def _slug_filename(title: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", title).strip("_")
    digest = hashlib.sha1(title.encode("utf-8")).hexdigest()[:8]
    return f"{slug or 'page'}_{digest}.json"


def _persist_row(persist_dir: Optional[str], row: Dict[str, object]) -> None:
    if not persist_dir:
        return
    Path(persist_dir).mkdir(parents=True, exist_ok=True)
    fname = _slug_filename(str(row.get("title", "page")))
    target = Path(persist_dir) / fname
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(row, ensure_ascii=False))
    tmp.replace(target)


@ray.remote(num_cpus=0.25, max_retries=3, retry_exceptions=True)
def fetch_and_tokenize(
    title: str,
    endpoint: str,
    encoding_name: str,
    user_agent: str,
    persist_dir: Optional[str],
    tokens_dir: Optional[str],
) -> Dict[str, object]:
    # Skip if output already exists (idempotent task execution)
    if tokens_dir:
        output_path = Path(tokens_dir) / _slug_filename(title)
        if output_path.exists():
            return {
                "title": title,
                "status": "skipped",
                "reason": "output_exists",
                "tokens_path": str(output_path),
            }

    start = time.time()
    try:
        text = fetch_plaintext(title, endpoint, user_agent=user_agent)
        token_ids = tokenize_text(text, encoding_name)
        duration = time.time() - start
        tokens_path = None
        if tokens_dir:
            Path(tokens_dir).mkdir(parents=True, exist_ok=True)
            tokens_path = Path(tokens_dir) / _slug_filename(title)
            tmp_tokens = tokens_path.with_suffix(".tmp")
            tmp_tokens.write_text(json.dumps({"title": title, "tokens": token_ids}, ensure_ascii=False))
            tmp_tokens.replace(tokens_path)
        row = {
            "title": title,
            "token_count": len(token_ids),
            "chars": len(text),
            "duration_sec": round(duration, 3),
            "status": "ok",
            "tokens_path": str(tokens_path) if tokens_path else None,
        }
        _persist_row(persist_dir, row)
        return row
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        row = {
            "title": title,
            "status": "error",
            "error": str(exc),
            "duration_sec": round(duration, 3),
        }
        _persist_row(persist_dir, row)
        return row


def write_jsonl(path: str, rows: Iterable[Dict[str, object]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_checkpoint(checkpoint_path: Optional[str]) -> Set[str]:
    """Load completed titles from checkpoint file."""
    if not checkpoint_path:
        return set()
    path = Path(checkpoint_path)
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text())
        completed = set(data.get("completed", []))
        print(f"Loaded checkpoint with {len(completed)} completed titles")
        return completed
    except (json.JSONDecodeError, IOError) as e:
        print(f"Warning: Could not load checkpoint {checkpoint_path}: {e}")
        return set()


def save_checkpoint(checkpoint_path: Optional[str], completed_titles: List[str]) -> None:
    """Save completed titles to checkpoint file (atomic write)."""
    if not checkpoint_path:
        return
    path = Path(checkpoint_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    data = {
        "completed": completed_titles,
        "count": len(completed_titles),
        "timestamp": time.time(),
    }
    tmp.write_text(json.dumps(data, ensure_ascii=False))
    tmp.replace(path)


def process_titles(
    titles: List[str],
    endpoint: str,
    encoding_name: str,
    concurrency: int,
    user_agent: str,
    persist_dir: Optional[str],
    tokens_dir: Optional[str],
    checkpoint_path: Optional[str] = None,
    show_progress: bool = True,
) -> List[Dict[str, object]]:
    inflight: List[ray.ObjectRef] = []
    results: List[Dict[str, object]] = []
    completed_titles: List[str] = []
    total = len(titles)
    completed = 0
    ok_count = 0
    skipped_count = 0
    err_count = 0
    checkpoint_interval = 10  # Save checkpoint every N completions

    def log_progress(batch: List[Dict[str, object]]) -> None:
        nonlocal completed, ok_count, skipped_count, err_count
        for row in batch:
            completed += 1
            status = row.get("status")
            title = str(row.get("title", ""))
            if status == "ok":
                ok_count += 1
                completed_titles.append(title)
            elif status == "skipped":
                skipped_count += 1
                completed_titles.append(title)  # Still counts as done
            else:
                err_count += 1
        if show_progress and batch:
            last = batch[-1]
            print(
                f"[{completed}/{total}] ok={ok_count} skip={skipped_count} err={err_count} last={last.get('title')} ({last.get('status')})",
                flush=True,
            )
        # Save checkpoint periodically
        if checkpoint_path and completed % checkpoint_interval == 0:
            save_checkpoint(checkpoint_path, completed_titles)

    for title in titles:
        inflight.append(fetch_and_tokenize.remote(title, endpoint, encoding_name, user_agent, persist_dir, tokens_dir))
        if len(inflight) >= concurrency:
            ready, inflight = ray.wait(inflight, num_returns=1)
            batch_rows = ray.get(ready)
            results.extend(batch_rows)
            log_progress(batch_rows)
    while inflight:
        ready, inflight = ray.wait(inflight, num_returns=1)
        batch_rows = ray.get(ready)
        results.extend(batch_rows)
        log_progress(batch_rows)

    # Final checkpoint save
    if checkpoint_path:
        save_checkpoint(checkpoint_path, completed_titles)

    return results


def summarize(rows: List[Dict[str, object]]) -> Dict[str, object]:
    ok_rows = [r for r in rows if r.get("status") == "ok"]
    skipped = [r for r in rows if r.get("status") == "skipped"]
    errors = [r for r in rows if r.get("status") not in ("ok", "skipped")]
    total_tokens = sum(r["token_count"] for r in ok_rows)
    return {
        "pages": len(rows),
        "ok": len(ok_rows),
        "skipped": len(skipped),
        "errors": len(errors),
        "total_tokens": total_tokens,
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Ray job to fetch and tokenize Wikipedia pages.")
    parser.add_argument("--pages", help="Comma-separated page titles.")
    parser.add_argument("--pages-file", help="Path to file with newline-delimited titles.")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="tiktoken encoding name.")
    parser.add_argument("--endpoint", default=WIKI_API_ENDPOINT, help="Wikipedia API endpoint.")
    parser.add_argument("--concurrency", type=int, default=1, help="Max concurrent fetch/tokenize tasks.")
    parser.add_argument("--max-pages", type=int, help="Limit number of pages processed.")
    parser.add_argument("--output", default="/tmp/token_counts.jsonl", help="Path to JSONL output.")
    parser.add_argument(
        "--output-dir",
        help="Optional directory to persist per-page JSON rows as they complete (use a durable PVC/object mount in K8s).",
    )
    parser.add_argument(
        "--tokens-dir",
        help="Optional directory to persist per-page token payloads as they complete (use a durable PVC/object mount in K8s).",
    )
    parser.add_argument(
        "--checkpoint",
        help="Path to checkpoint file for resume support (e.g., /output/checkpoint.json).",
    )
    args = parser.parse_args(argv)

    titles = load_titles(args)
    if args.max_pages:
        titles = titles[: args.max_pages]
    if not titles:
        print("No pages provided.")
        return 1

    # Load checkpoint and filter out already completed titles
    already_completed = load_checkpoint(args.checkpoint)
    original_count = len(titles)
    if already_completed:
        titles = [t for t in titles if t not in already_completed]
        skipped_from_checkpoint = original_count - len(titles)
        if skipped_from_checkpoint > 0:
            print(f"Skipping {skipped_from_checkpoint} pages from checkpoint (already completed)")

    if not titles:
        print("All pages already completed (from checkpoint). Nothing to do.")
        return 0

    # Connect to the existing Ray cluster (RayJob sets this up).
    ray.init(address="auto")
    print(f"Processing {len(titles)} pages with concurrency={args.concurrency} and encoding={args.encoding}")
    rows = process_titles(
        titles,
        args.endpoint,
        args.encoding,
        args.concurrency,
        USER_AGENT,
        args.output_dir,
        args.tokens_dir,
        checkpoint_path=args.checkpoint,
        show_progress=True,
    )
    stats = summarize(rows)
    write_jsonl(args.output, rows)

    print(json.dumps(stats, indent=2))
    print(f"Wrote per-page stats to {args.output}")
    if args.checkpoint:
        print(f"Checkpoint saved to {args.checkpoint}")
    if stats["skipped"]:
        print(f"{stats['skipped']} pages skipped (output already exists).")
    if stats["errors"]:
        print(f"{stats['errors']} pages failed; check output for details.")
    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
