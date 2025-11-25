#!/bin/bash
#
# Download Wikipedia CirrusSearch dumps for local processing or manual staging.
#
# Usage:
#   ./scripts/download_dumps.sh [output_dir] [wiki_name] [dump_date]
#
# Examples:
#   # Download latest English Wikipedia dump to ./dumps/
#   ./scripts/download_dumps.sh
#
#   # Download to specific directory
#   ./scripts/download_dumps.sh /data/wiki-dumps
#
#   # Download Simple Wikipedia (smaller, good for testing)
#   ./scripts/download_dumps.sh ./dumps simplewiki
#
#   # Download specific date
#   ./scripts/download_dumps.sh ./dumps enwiki 20241118
#
# The script downloads the "content" dump which contains article text.
# For English Wikipedia, this is ~20GB compressed.

set -e

DUMP_DIR="${1:-./dumps}"
WIKI="${2:-enwiki}"
DUMP_DATE="${3:-current}"

BASE_URL="https://dumps.wikimedia.org/other/cirrussearch"

# Create output directory
mkdir -p "$DUMP_DIR"

echo "=============================================="
echo "Wikipedia CirrusSearch Dump Downloader"
echo "=============================================="
echo "Wiki:       $WIKI"
echo "Date:       $DUMP_DATE"
echo "Output dir: $DUMP_DIR"
echo ""

# Build the URL - find latest date if "current" is specified
if [ "$DUMP_DATE" = "current" ]; then
    echo "Finding latest dump date..."
    # Get the most recent date directory (they're in YYYYMMDD format)
    LATEST_DATE=$(curl -sL "$BASE_URL/" | grep -oE '20[0-9]{6}' | sort -r | head -1)
    if [ -z "$LATEST_DATE" ]; then
        echo "ERROR: Could not determine latest dump date"
        exit 1
    fi
    echo "Latest dump date: $LATEST_DATE"
    INDEX_URL="$BASE_URL/$LATEST_DATE/"
else
    INDEX_URL="$BASE_URL/$DUMP_DATE/"
fi

echo "Fetching dump index from $INDEX_URL..."

# Find the dump filename from the directory listing
# Pattern: enwiki-20241118-cirrussearch-content.json.gz
DUMP_FILE=$(curl -sL "$INDEX_URL" | grep -oE "${WIKI}-[0-9]+-cirrussearch-content\.json\.gz" | head -1)

if [ -z "$DUMP_FILE" ]; then
    echo "ERROR: Could not find dump file for $WIKI in $INDEX_URL"
    echo ""
    echo "Available dumps:"
    curl -sL "$INDEX_URL" | grep -oE '[a-z]+wiki-[0-9]+-cirrussearch-content\.json\.gz' | sort -u | head -20
    exit 1
fi

DUMP_URL="$INDEX_URL$DUMP_FILE"
OUTPUT_PATH="$DUMP_DIR/$DUMP_FILE"

echo "Found dump: $DUMP_FILE"
echo ""

# Check if already downloaded
if [ -f "$OUTPUT_PATH" ]; then
    echo "File already exists: $OUTPUT_PATH"
    echo "Size: $(ls -lh "$OUTPUT_PATH" | awk '{print $5}')"
    echo ""
    read -p "Re-download? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping download."
        exit 0
    fi
fi

echo "Downloading $DUMP_URL"
echo "This may take 30-60 minutes for English Wikipedia (~20GB)"
echo ""

# Download with progress bar and resume support
curl -L -C - --progress-bar -o "$OUTPUT_PATH" "$DUMP_URL"

echo ""
echo "=============================================="
echo "Download complete!"
echo "=============================================="
echo "File: $OUTPUT_PATH"
echo "Size: $(ls -lh "$OUTPUT_PATH" | awk '{print $5}')"
echo ""
echo "To test locally:"
echo "  python -m ray_app.cirrus_reader $OUTPUT_PATH --max 10"
echo ""
echo "To process with Ray:"
echo "  python -m ray_app.tokenize_wiki --cirrus-file $OUTPUT_PATH --max-pages 100"

