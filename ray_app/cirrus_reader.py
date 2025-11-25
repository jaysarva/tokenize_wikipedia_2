"""
CirrusSearch dump reader for Wikipedia.

Streams articles from CirrusSearch NDJSON dump files (gzipped).
These dumps contain pre-extracted plaintext from Wikipedia's search index.

Dump format (NDJSON with alternating lines):
  {"index": {"_type": "page", "_id": "12345"}}
  {"page_id": 12345, "title": "Example", "text": "...", "namespace": 0, ...}
  {"index": {"_type": "page", "_id": "12346"}}
  {"page_id": 12346, "title": "Another", "text": "...", "namespace": 0, ...}

Usage:
    from ray_app.cirrus_reader import stream_cirrus_dir, stream_cirrus_dump

    # Stream all articles from a directory of dump files
    for article in stream_cirrus_dir("/dumps"):
        print(article["title"], len(article["text"]))

    # Stream from a single dump file
    for article in stream_cirrus_dump("/dumps/enwiki-content.json.gz"):
        print(article["title"])
"""

import gzip
import json
import logging
from pathlib import Path
from typing import Dict, Iterator, Optional, Union

log = logging.getLogger(__name__)


class CirrusParseError(Exception):
    """Raised when a dump line cannot be parsed."""


def stream_cirrus_dump(
    path: Union[str, Path],
    namespace_filter: Optional[int] = 0,
    max_articles: Optional[int] = None,
) -> Iterator[Dict]:
    """
    Stream articles from a single CirrusSearch dump file.

    Args:
        path: Path to a .json.gz or .json.bz2 dump file
        namespace_filter: Only yield articles from this namespace (0 = main articles).
                         Set to None to yield all namespaces.
        max_articles: Maximum number of articles to yield (for testing)

    Yields:
        Dict with keys: page_id, title, text, namespace, timestamp (if available)
    """
    path = Path(path)
    count = 0
    errors = 0
    skipped_ns = 0

    # Determine compression type
    if path.suffix == ".gz" or path.name.endswith(".json.gz"):
        opener = lambda p: gzip.open(p, "rt", encoding="utf-8")
    elif path.suffix == ".bz2" or path.name.endswith(".json.bz2"):
        import bz2
        opener = lambda p: bz2.open(p, "rt", encoding="utf-8")
    else:
        # Assume uncompressed JSON
        opener = lambda p: open(p, "r", encoding="utf-8")

    log.info(f"Streaming from {path}")

    with opener(path) as f:
        line_num = 0
        while True:
            # Read index line (metadata)
            index_line = f.readline()
            line_num += 1
            if not index_line:
                break  # EOF

            # Read document line
            doc_line = f.readline()
            line_num += 1
            if not doc_line:
                log.warning(f"Unexpected EOF at line {line_num} (missing doc after index)")
                break

            # Parse document
            try:
                doc = json.loads(doc_line)
            except json.JSONDecodeError as e:
                errors += 1
                if errors <= 10:
                    log.warning(f"JSON parse error at line {line_num}: {e}")
                elif errors == 11:
                    log.warning("Suppressing further JSON parse errors...")
                continue

            # Filter by namespace
            ns = doc.get("namespace")
            if namespace_filter is not None and ns != namespace_filter:
                skipped_ns += 1
                continue

            # Extract required fields
            page_id = doc.get("page_id")
            title = doc.get("title")
            text = doc.get("text") or doc.get("source_text") or ""

            if not title:
                errors += 1
                if errors <= 10:
                    log.warning(f"Missing title at line {line_num}, page_id={page_id}")
                continue

            if not text:
                errors += 1
                if errors <= 10:
                    log.warning(f"Empty text for '{title}' (page_id={page_id})")
                continue

            count += 1
            yield {
                "page_id": page_id,
                "title": title,
                "text": text,
                "namespace": ns,
                "timestamp": doc.get("timestamp"),
            }

            if max_articles and count >= max_articles:
                log.info(f"Reached max_articles limit ({max_articles})")
                break

    log.info(
        f"Finished {path.name}: yielded {count} articles, "
        f"skipped {skipped_ns} (wrong namespace), {errors} errors"
    )


def stream_cirrus_dir(
    dir_path: Union[str, Path],
    namespace_filter: Optional[int] = 0,
    max_articles: Optional[int] = None,
    file_pattern: str = "*cirrussearch-content*.json.gz",
) -> Iterator[Dict]:
    """
    Stream articles from all CirrusSearch dump files in a directory.

    Args:
        dir_path: Directory containing .json.gz dump files
        namespace_filter: Only yield articles from this namespace (0 = main)
        max_articles: Maximum total articles to yield across all files
        file_pattern: Glob pattern to match dump files

    Yields:
        Dict with keys: page_id, title, text, namespace, timestamp
    """
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        raise ValueError(f"Not a directory: {dir_path}")

    # Find all dump files
    dump_files = sorted(dir_path.glob(file_pattern))
    
    # Also check for bz2 files (new format)
    if not dump_files:
        dump_files = sorted(dir_path.glob("*cirrussearch-content*.json.bz2"))
    
    # Fallback: any .json.gz file
    if not dump_files:
        dump_files = sorted(dir_path.glob("*.json.gz"))

    if not dump_files:
        raise ValueError(f"No dump files found in {dir_path} matching pattern")

    log.info(f"Found {len(dump_files)} dump file(s) in {dir_path}")

    total_count = 0
    for dump_file in dump_files:
        remaining = None
        if max_articles:
            remaining = max_articles - total_count
            if remaining <= 0:
                break

        for article in stream_cirrus_dump(
            dump_file,
            namespace_filter=namespace_filter,
            max_articles=remaining,
        ):
            total_count += 1
            yield article

            if max_articles and total_count >= max_articles:
                return

    log.info(f"Total articles streamed from {len(dump_files)} files: {total_count}")


def count_articles(
    path: Union[str, Path],
    namespace_filter: Optional[int] = 0,
) -> int:
    """
    Count articles in a dump file or directory without loading text.
    Useful for progress estimation.
    """
    path = Path(path)
    if path.is_dir():
        return sum(
            1 for _ in stream_cirrus_dir(path, namespace_filter=namespace_filter)
        )
    else:
        return sum(
            1 for _ in stream_cirrus_dump(path, namespace_filter=namespace_filter)
        )


def get_dump_info(path: Union[str, Path]) -> Dict:
    """
    Get metadata about a dump file or directory.
    Returns file count, estimated article count (from first 1000 lines), file sizes.
    """
    path = Path(path)
    info = {
        "path": str(path),
        "is_directory": path.is_dir(),
        "files": [],
        "total_size_bytes": 0,
        "sample_count": 0,
    }

    if path.is_dir():
        dump_files = list(path.glob("*cirrussearch*.json.gz")) + list(
            path.glob("*cirrussearch*.json.bz2")
        )
        for f in dump_files:
            size = f.stat().st_size
            info["files"].append({"name": f.name, "size_bytes": size})
            info["total_size_bytes"] += size
    else:
        info["files"].append({
            "name": path.name,
            "size_bytes": path.stat().st_size,
        })
        info["total_size_bytes"] = path.stat().st_size

    # Sample first 1000 articles to estimate total
    try:
        if path.is_dir():
            info["sample_count"] = sum(
                1 for _ in stream_cirrus_dir(path, max_articles=1000)
            )
        else:
            info["sample_count"] = sum(
                1 for _ in stream_cirrus_dump(path, max_articles=1000)
            )
    except Exception as e:
        info["sample_error"] = str(e)

    return info


if __name__ == "__main__":
    # CLI for testing
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="CirrusSearch dump reader")
    parser.add_argument("path", help="Path to dump file or directory")
    parser.add_argument("--max", type=int, default=10, help="Max articles to show")
    parser.add_argument("--info", action="store_true", help="Show dump info only")
    parser.add_argument("--count", action="store_true", help="Count all articles")
    parser.add_argument("--namespace", type=int, default=0, help="Namespace filter")
    args = parser.parse_args()

    path = Path(args.path)

    if args.info:
        info = get_dump_info(path)
        print(json.dumps(info, indent=2))
        sys.exit(0)

    if args.count:
        print(f"Counting articles (namespace={args.namespace})...")
        total = count_articles(path, namespace_filter=args.namespace)
        print(f"Total: {total}")
        sys.exit(0)

    # Stream and display sample articles
    if path.is_dir():
        stream = stream_cirrus_dir(path, namespace_filter=args.namespace, max_articles=args.max)
    else:
        stream = stream_cirrus_dump(path, namespace_filter=args.namespace, max_articles=args.max)

    for i, article in enumerate(stream, 1):
        print(f"\n{'='*60}")
        print(f"[{i}] {article['title']} (page_id={article['page_id']})")
        print(f"    namespace={article['namespace']}, text_len={len(article['text'])}")
        print(f"    text preview: {article['text'][:200]}...")

