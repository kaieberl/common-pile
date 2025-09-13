#!/usr/bin/env sh

set -e

python hf-to-dolma.py
python preprocess.py
hf upload kai271/arxiv-papers-encoded data/arxiv/v0/documents . --repo-type dataset
