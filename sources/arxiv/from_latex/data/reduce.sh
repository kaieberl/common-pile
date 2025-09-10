#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
ID_FILE="arxiv-shards.txt"
HF_REPO="kai271/arxiv-papers"
WORK_DIR="data"

# --- Pre-flight checks ---
# Check for required command-line tools
command -v aws >/dev/null 2>&1 || { echo >&2 "AWS CLI ('aws') not found. Please install and configure it. Aborting."; exit 1; }
command -v huggingface-cli >/dev/null 2>&1 || { echo >&2 "Hugging Face CLI ('huggingface-cli') not found. Please install it ('pip install huggingface_hub'). Aborting."; exit 1; }

# Create a temporary working directory
mkdir -p "$WORK_DIR"

while IFS= read -r ID || [[ -n "$ID" ]]; do
    echo "--- Processing Shard ID: $ID ---"

    # Define file and directory names
    TAR_FILE="${ID}.tar"
    SHARD_DIR="${WORK_DIR}/${ID}"
    OUTPUT_FILE="${ID}_tex.tar.gz"

    # 1. Download the shard from S3
    echo "1. Downloading s3://arxiv/src/$TAR_FILE..."  # e.g. s3://arxiv/src/arXiv_src_1001_001.tar
    aws s3 cp "s3://arxiv/src/$TAR_FILE" "$WORK_DIR/" --request-payer requester

    # Create a directory for unpacking
    mkdir -p "$SHARD_DIR"

    # 2. Unpack the downloaded tar file
    echo "2. Unpacking $TAR_FILE..."
    tar -xf "$WORK_DIR/$TAR_FILE" -C "$SHARD_DIR"

    # 3. Unpack all .gz files within the shard directory
    echo "3. Unpacking .gz files..."
    find "$SHARD_DIR" -type f -name "*.gz" -print0 | while IFS= read -r -d $'\0' gz_file; do
        if tar -tzf "$gz_file" >/dev/null 2>&1; then
            echo "[PID: $$]    Unpacking $gz_file"
            # Extract in the same directory as the archive.
            # The 'z' flag handles gzip decompression. 'xvf' extracts verbose file list.
            # We also remove the archive after extraction to mimic gunzip's behavior.
            mkdir "${gz_file%.gz}"
            tar -xzf "$gz_file" -C "${gz_file%.gz}" && rm "$gz_file"
        else
            gunzip -c "$gz_file" > "${gz_file%.gz}.tex"
        fi
    done

    # 4. Delete all files except for .tex files
    echo "4. Deleting non-.tex files..."
    find "$SHARD_DIR" -type f -not -name "*.tex" -delete

    # 5. Pack the resulting .tex files back into a .tar.gz file
    echo "5. Packing .tex files into $OUTPUT_FILE..."
    tar -czf "$OUTPUT_FILE" -C "$SHARD_DIR" .

    # 6. Upload the file to Hugging Face
    echo "6. Uploading $OUTPUT_FILE to Hugging Face repository: $HF_REPO"
    hf upload "$HF_REPO" "$OUTPUT_FILE" "shards/$OUTPUT_FILE" --repo-type dataset

    # Clean up local files for the current shard
    echo "Cleaning up local files for shard $ID..."
    rm -f "$WORK_DIR/$TAR_FILE"
    rm -rf "$SHARD_DIR"
    rm -f "$OUTPUT_FILE"

    echo "--- Finished Processing Shard ID: $ID ---"
    echo ""

done < "$ID_FILE"

# Clean up the working directory
rmdir "$WORK_DIR"

echo "All shards have been processed successfully."