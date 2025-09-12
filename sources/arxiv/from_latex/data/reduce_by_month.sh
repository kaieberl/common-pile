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

# Create a top-level working directory
mkdir -p "$WORK_DIR"

# --- Main Processing Loop ---
current_month=""
month_dir=""

# The input file is expected to be sorted so that all shards for a given month are consecutive.
# An extra newline is added to the input file read to ensure the last month is processed.
while IFS= read -r ID || [[ -n "$current_month" ]]; do
    # Extract month from ID, e.g., "arXiv_src_1509_001" -> "1509"
    shard_month=$(echo "$ID" | cut -d'_' -f3)

    # --- Month Change Detection ---
    # If the month changes (or it's the end of the file), process the completed month's data.
    if [[ "$shard_month" != "$current_month" && -n "$current_month" ]]; then
        echo "--- Finished processing shards for month: $current_month ---"

        # 1. Pack the entire month's .tex files into a single .tar.gz
        output_file="${current_month}_tex.tar.gz"
        echo "1. Packing .tex files into $output_file..."
        if [ -d "$month_dir" ] && [ "$(ls -A "$month_dir")" ]; then
            tar -czf "$output_file" -C "$month_dir" .

            # 2. Upload the consolidated archive to Hugging Face
            echo "2. Uploading $output_file to Hugging Face repository: $HF_REPO"
            if ! hf upload "$HF_REPO" "$output_file" "shards/$output_file" --repo-type dataset; then
                echo "Error: Failed to upload $output_file to Hugging Face. Aborting." >&2
                # Clean up the generated tar.gz file before exiting
                rm -f "$output_file"
                exit 1
            fi

            # 3. Clean up the month's archive
            rm -f "$output_file"
        else
            echo "No .tex files found for month $current_month. Skipping packaging and upload."
        fi

        # 4. Clean up the month's working directory
        echo "4. Cleaning up directory $month_dir..."
        rm -rf "$month_dir"

        # If we've processed all lines, exit the loop
        if [[ -z "$ID" ]]; then
            break
        fi
    fi

    # --- New Month Initialization ---
    # If a new month starts, set up the environment for it.
    if [[ "$shard_month" != "$current_month" ]]; then
        echo "--- Starting new month: $shard_month ---"
        current_month=$shard_month
        month_dir="${WORK_DIR}/${current_month}"
        mkdir -p "$month_dir"
    fi

    # --- Shard Processing ---
    echo "--- Processing Shard ID: $ID ---"

    # Define file path for the current shard
    tar_file_path="${WORK_DIR}/${ID}.tar"

    # 1. Download shard from S3
    echo "1. Downloading s3://arxiv/src/${ID}.tar..."
    aws s3 cp "s3://arxiv/src/${ID}.tar" "$WORK_DIR/" --request-payer requester

    # 2. Extract .tex files directly from the shard without unpacking it fully.
    echo "2. Extracting .tex files from ${ID}.tar..."
    tar -tf "$tar_file_path" | grep '\.gz$' | while read -r file; do
      if tar -xO -f "$tar_file_path" "$file" | tar -tzf - >/dev/null 2>&1; then
        # The inner .gz is actually a tar.gz. Extract only .tex files from it.
        output_dir="${month_dir}/$(basename "$file" .gz)"
        mkdir -p "$output_dir"
        tar -xO -f "$tar_file_path" "$file" | tar -xzf - -C "$output_dir" --wildcards --no-anchored "*.tex" || true

        # Clean up empty directories if no .tex files were found
        if [ -d "$output_dir" ] && [ -z "$(ls -A "$output_dir")" ]; then
            rmdir "$output_dir"
        fi
      else
        # The inner .gz is just a regular gzip file. Decompress it.
        output_tex_file="${month_dir}/$(basename "$file" .gz).tex"
        tar -xO -f "$tar_file_path" "$file" | gunzip -c > "$output_tex_file"
      fi
    done

    # 3. Clean up the downloaded shard tar file
    rm "$tar_file_path"

    echo "--- Finished Shard ID: $ID ---"
    echo ""

done < <(cat "$ID_FILE"; echo) # Add a newline to process the last month in the file

# Clean up the main working directory if it's empty
if [ -d "$WORK_DIR" ]; then
    rmdir --ignore-fail-on-non-empty "$WORK_DIR"
fi

echo "All shards have been processed successfully."