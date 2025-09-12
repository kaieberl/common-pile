#!/usr/bin/env sh

set -e

python bulk_download.py --manifest_only
unzip data/arxiv-metadata-oat-snapshot.json.zip
python hf-to-dolma.py
python preprocess.py
hf upload kai271/arxiv-papers-encoded data/arxiv/v0/documents . --repo-type dataset
