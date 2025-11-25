#!/usr/bin/env python
"""Fetch a handful of Wikipedia pages and tokenize them with tiktoken."""

import argparse
import json
import os
import sys
import time
import hashlib
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
import tiktoken
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


WIKI_API_ENDPOINT = "https://en.wikipedia.org/w/api.php"
DEFAULT_ENCODING = "cl100k_base"
USER_AGENT = os.environ.get("WIKI_USER_AGENT", "tokenize-wikipedia/0.1 (contact: your-email@example.com)")
DEFAULT_PAGES = [
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


class WikipediaFetchError(Exception):
    """Raised when a page cannot be fetched from the API."""


def load_titles(args: argparse.Namespace) -> List[str]:
    if args.pages:
        return [title.strip() for title in args.pages.split(",") if title.strip()]
    if args.pages_file:
        return [line.strip() for line in Path(args.pages_file).read_text().splitlines() if line.strip()]
    return DEFAULT_PAGES.copy()


@retry(
    retry=retry_if_exception_type(WikipediaFetchError),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
    stop=stop_after_attempt(4),
)
def fetch_plaintext(title: str, endpoint: str, session: requests.Session) -> str:
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


def tokenize_text(text: str, encoding_name: str) -> List[int]:
    encoding = tiktoken.get_encoding(encoding_name)
    return encoding.encode(text)


def write_jsonl(path: Optional[str], rows: Iterable[Dict[str, object]]) -> None:
    if not path:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def slug_filename(title: str, suffix: str = ".json") -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", title).strip("_")
    digest = hashlib.sha1(title.encode("utf-8")).hexdigest()[:8]
    return f"{slug or 'page'}_{digest}{suffix}"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and tokenize Wikipedia pages.")
    parser.add_argument("--pages", help="Comma-separated page titles.")
    parser.add_argument("--pages-file", help="Path to file with newline-delimited titles.")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="tiktoken encoding name.")
    parser.add_argument("--endpoint", default=WIKI_API_ENDPOINT, help="Wikipedia API endpoint.")
    parser.add_argument("--max-pages", type=int, help="Limit number of pages processed.")
    parser.add_argument("--output", help="Path to JSONL output of per-page stats.")
    parser.add_argument("--tokens-dir", help="Optional directory to write per-page token files.")
    args = parser.parse_args(argv)

    titles = load_titles(args)
    if args.max_pages:
        titles = titles[: args.max_pages]
    if not titles:
        print("No pages provided.", file=sys.stderr)
        return 1

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    rows = []
    total_tokens = 0
    errors = 0
    for title in titles:
        start = time.time()
        try:
            text = fetch_plaintext(title, args.endpoint, session=session)
            tokens = tokenize_text(text, args.encoding)
            duration = time.time() - start
            total_tokens += len(tokens)
            token_path = None
            if args.tokens_dir:
                Path(args.tokens_dir).mkdir(parents=True, exist_ok=True)
                token_path = Path(args.tokens_dir) / slug_filename(title, suffix=".tokens.json")
                payload = {"title": title, "tokens": tokens}
                token_path.write_text(json.dumps(payload, ensure_ascii=False))
            row = {
                "title": title,
                "token_count": len(tokens),
                "duration_sec": round(duration, 3),
                "chars": len(text),
                "status": "ok",
                "tokens_path": str(token_path) if token_path else None,
            }
            rows.append(row)
            print(f"{title}: {row['token_count']} tokens in {row['duration_sec']}s ({row['chars']} chars)")
        except Exception as exc:  # noqa: BLE001
            duration = time.time() - start
            errors += 1
            row = {
                "title": title,
                "status": "error",
                "error": str(exc),
                "duration_sec": round(duration, 3),
            }
            rows.append(row)
            print(f"{title}: ERROR {exc}", file=sys.stderr)

    write_jsonl(args.output, rows)
    print(f"Processed {len(rows)} pages. Total tokens: {total_tokens}. Errors: {errors}.")
    if args.output:
        print(f"Wrote per-page stats to {args.output}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
