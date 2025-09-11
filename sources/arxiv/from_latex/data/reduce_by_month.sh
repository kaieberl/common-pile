#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Argument Parsing ---
usage() {
    echo "Usage: $0 [ID_FILE_PATH]"
    echo
    echo "Processes arXiv source files based on a list of shard IDs."
    echo "If ID_FILE_PATH is not provided, it defaults to 'arxiv-shards.txt'."
    echo
    echo "  -h, --help     Show this help message and exit."
}

# Default value for the ID file
ID_FILE="arxiv-shards.txt"

# Handle command-line arguments
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
    exit 0
fi

if [ -n "$1" ]; then
    ID_FILE="$1"
fi

# --- Configuration ---
HF_REPO="kai271/arxiv-papers"
WORK_DIR="data"

# --- Pre-flight checks ---
command -v aws >/dev/null 2>&1 || { echo >&2 "AWS CLI ('aws') not found. Please install and configure it. Aborting."; exit 1; }
command -v huggingface-cli >/dev/null 2>&1 || { echo >&2 "Hugging Face CLI ('huggingface-cli') not found. Please install it ('pip install huggingface_hub'). Aborting."; exit 1; }
command -v tar >/dev/null 2>&1 || { echo >&2 "GNU tar ('tar') is required. Aborting."; exit 1; }

# Create a top-level working directory
mkdir -p "$WORK_DIR"

# --- Main Processing Loop ---
current_month=""
month_archive=""
output_dir=""

# This function finalizes the processing for a given month
process_month_end() {
    if [[ -n "$current_month" ]]; then
        echo "--- Finished processing shards for month: $current_month ---"

        # 1. Finalize the month's .tex files into a single .tar.gz
        output_file="${output_dir}/${current_month}_tex.tar.gz"
        echo "1. Compressing collected .tex files into $output_file..."
        tar -czf "$output_file" -C "$output_dir" .

        # 2. Upload the consolidated archive to Hugging Face
        echo "2. Uploading $output_file to Hugging Face repository: $HF_REPO"
        if ! hf upload "$HF_REPO" "$output_file" "shards/$output_file" --repo-type dataset; then
            echo "Error: Failed to upload $output_file to Hugging Face. Aborting." >&2
            rm -rf "$output_dir"
            exit 1
        fi

        # 3. Clean up the month's working directory
        echo "3. Cleaning up directory $output_dir..."
        rm -rf "$output_dir"
    fi
}

# The input file is expected to be sorted so that all shards for a given month are consecutive.
while IFS= read -r ID; do
    # Extract month from ID, e.g., "arXiv_src_1509_001" -> "1509"
    shard_month=$(echo "$ID" | cut -d'_' -f3)

    # --- Month Change Detection ---
    if [[ "$shard_month" != "$current_month" ]]; then
        process_month_end # Finalize the previous month

        echo "--- Starting new month: $shard_month ---"
        current_month=$shard_month
        output_dir="${WORK_DIR}/${current_month}"
        mkdir -p "$output_dir"
    fi

    # --- Shard Processing ---
    echo "--- Processing Shard ID: $ID ---"
    tar_file="${ID}.tar"
    shard_temp_dir="${WORK_DIR}/${ID}_temp"
    mkdir -p "$shard_temp_dir"

    # 1. Download shard from S3
    echo "1. Downloading s3://arxiv/src/$tar_file..."
    aws s3 cp "s3://arxiv/src/$tar_file" "$WORK_DIR/" --request-payer requester

    # 2. Unpack the downloaded tar file
    echo "2. Unpacking $tar_file..."
    tar -xf "$WORK_DIR/$tar_file" -C "$shard_temp_dir"
    rm "$WORK_DIR/$tar_file" # Clean up tar file immediately

    # 3. Efficiently extract .tex files
    echo "3. Extracting .tex files..."
    find "$shard_temp_dir" -type f -name "*.gz" -print0 | while IFS= read -r -d $'\0' gz_file; do
        # Create a unique name for the content based on the .gz filename
        output_basename=$(basename "${gz_file%.gz}")

        # Directly extract only .tex files into a subdirectory within the month's output dir
        # This is much faster than extracting everything and then deleting.
        # It also handles both .tar.gz and simple .gz files gracefully.
        # We create a subdirectory for each .gz to avoid filename collisions.
        dest_path="${output_dir}/${output_basename}"
        mkdir -p "$dest_path"

        # Attempt to extract as a tar archive, filtering for .tex files.
        # The `--wildcards` and `--strip-components` flags are powerful features of GNU tar.
        # If it's not a valid tar archive, gunzip will be used as a fallback.
        if tar -tzf "$gz_file" &>/dev/null; then
             # It's a tar.gz file. Extract only .tex files.
            tar -xzf "$gz_file" -C "$dest_path" --wildcards --no-anchor '*.tex' --strip-components=1 2>/dev/null || \
            tar -xzf "$gz_file" -C "$dest_path" --wildcards --no-anchor '*.tex' # Fallback for archives without a top-level dir
        else
            # It's a simple .gz file. Decompress it.
            # We check if the uncompressed name ends with .tex implicitly
            if [[ "$(basename "$output_basename")" == *.tex ]]; then
                gunzip -c "$gz_file" > "${dest_path}.tex"
            fi
        fi

        # Clean up empty directories if extraction resulted in no .tex files
        find "$dest_path" -depth -type d -empty -delete 2>/dev/null || true
    done

    # 4. Clean up the temporary shard directory
    rm -rf "$shard_temp_dir"

    echo "--- Finished Shard ID: $ID ---"
    echo ""

done < "$ID_FILE"

# Process the very last month in the file
process_month_end

# Clean up the main working directory if it's empty
if [ -d "$WORK_DIR" ]; then
    rmdir --ignore-fail-on-non-empty "$WORK_DIR"
fi

echo "All shards have been processed successfully."