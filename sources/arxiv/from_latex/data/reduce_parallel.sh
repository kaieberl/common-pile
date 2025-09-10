#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
ID_FILE="arxiv-shards.txt"
HF_REPO="kai271/arxiv-papers"
WORK_DIR="data"
MAX_PARALLEL_JOBS=4

# --- Pre-flight checks ---
# Check for required command-line tools
command -v aws >/dev/null 2>&1 || { echo >&2 "AWS CLI ('aws') not found. Please install and configure it. Aborting."; exit 1; }
command -v huggingface-cli >/dev/null 2>&1 || { echo >&2 "Hugging Face CLI ('huggingface-cli') not found. Please install it ('pip install huggingface_hub'). Aborting."; exit 1; }
command -v xargs >/dev/null 2>&1 || { echo >&2 "xargs not found. It is usually installed by default on most systems. Aborting."; exit 1; }

# --- Processing Function ---
# This function contains the logic to process a single shard.
process_shard() {
    ID=$1
    echo "--- [PID: $$] Starting Shard ID: $ID ---"

    # Define file and directory names
    TAR_FILE="${ID}.tar"
    SHARD_DIR="${WORK_DIR}/${ID}"
    OUTPUT_FILE="${ID}_tex.tar.gz"

    # 1. Download the shard from S3
    echo "[PID: $$] 1. ($ID) Downloading s3://arxiv/src/$TAR_FILE..."
    aws s3 cp "s3://arxiv/src/$TAR_FILE" "$WORK_DIR/" --request-payer requester

    # Create a directory for unpacking
    mkdir -p "$SHARD_DIR"

    # 2. Unpack the downloaded tar file
    echo "[PID: $$] 2. ($ID) Unpacking $TAR_FILE..."
    tar -xf "$WORK_DIR/$TAR_FILE" -C "$SHARD_DIR"

    # 3. Unpack all .gz files within the shard directory
    echo "[PID: $$] 3. ($ID) Unpacking .gz files..."
    # The find command can sometimes fail if no .gz files are found.
    # We add '|| true' to prevent the script from exiting if that's the case.
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
    echo "[PID: $$] 4. ($ID) Deleting non-.tex files..."
    find "$SHARD_DIR" -type f -not -name "*.tex" -delete

    # 5. Pack the resulting .tex files back into a .tar.gz file
    echo "[PID: $$] 5. ($ID) Packing .tex files into $OUTPUT_FILE..."
    tar -czf "$OUTPUT_FILE" -C "$SHARD_DIR" .

    # 6. Upload the file to Hugging Face
    echo "[PID: $$] 6. ($ID) Uploading $OUTPUT_FILE to Hugging Face repository: $HF_REPO"
    hf upload "$HF_REPO" "$OUTPUT_FILE" "shards/$OUTPUT_FILE" --repo-type dataset

    # Clean up local files for the current shard
    echo "[PID: $$] Cleaning up local files for shard $ID..."
    rm -f "$WORK_DIR/$TAR_FILE"
    rm -rf "$SHARD_DIR"
    rm -f "$OUTPUT_FILE"

    echo "--- [PID: $$] Finished Processing Shard ID: $ID ---"
}

# Export the function and variables so they are available to the sub-shells created by xargs
export -f process_shard
export WORK_DIR
export HF_REPO

# --- Main processing loop ---
echo "Starting parallel shard processing with up to $MAX_PARALLEL_JOBS jobs..."

# Create the main working directory
mkdir -p "$WORK_DIR"

# Use xargs to run the processing function in parallel
# -n 1: Pass one ID at a time to the command.
# -P $MAX_PARALLEL_JOBS: Set the maximum number of parallel processes.
# -I {}: Replace {} with the input ID.
# The 'bash -c' part is used to execute the exported bash function.
cat "$ID_FILE" | xargs -n 1 -P "$MAX_PARALLEL_JOBS" -I {} bash -c 'process_shard "{}"'

# Clean up the main working directory if it's empty
rmdir "$WORK_DIR" 2>/dev/null || echo "Working directory '$WORK_DIR' not empty. Manual cleanup may be needed."

echo "All shards have been processed."