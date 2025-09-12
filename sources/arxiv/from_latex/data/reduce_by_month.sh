#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e
# Exit on pipe failures
set -o pipefail

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
            huggingface-cli upload "$HF_REPO" "$output_file" "shards/$output_file" --repo-type dataset

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

    # Define file and directory names for the current shard
    tar_file="${ID}.tar"
    s3_path="s3://arxiv/src/$tar_file"

    echo "1. Streaming and processing from $s3_path..."

    # Stream the tar file from S3, then pipe it to a loop that processes each .gz file inside.
    aws s3 cp "$s3_path" - --request-payer requester | \
    while IFS= read -r gz_path; do

        # Extract the specific .gz file from the stream to stdout
        inner_gz_stream=$(aws s3 cp "$s3_path" - --request-payer requester | tar -xO --no-recursion "$gz_path")

        # Check if the inner .gz is a tar archive
        if echo "$inner_gz_stream" | tar -tz >/dev/null 2>&1; then
            # It's a .tar.gz. Extract only .tex files into a dedicated directory.
            output_dir="${month_dir}/$(basename "${gz_path%.gz}")"
            mkdir -p "$output_dir"
            echo "$inner_gz_stream" | tar -xz -C "$output_dir" --wildcards --no-anchored "*.tex"

            # Clean up empty directories if no .tex files were found
            find "$output_dir" -depth -type d -empty -delete
        else
            # It's a simple .gz. Decompress it to a .tex file.
            output_tex_file="${month_dir}/$(basename "${gz_path%.gz}").tex"
            echo "$inner_gz_stream" | gunzip -c > "$output_tex_file"

            # Remove empty .tex files
            if [ ! -s "$output_tex_file" ]; then
                rm -f "$output_tex_file"
            fi
        fi
    done < <(aws s3 cp "$s3_path" - --request-payer requester | tar -t | grep '\.gz$')

    echo "--- Finished Shard ID: $ID ---"
    echo ""

done < <(cat "$ID_FILE"; echo) # Add a newline to process the last month in the file

# Clean up the main working directory if it's empty
if [ -d "$WORK_DIR" ]; then
    rmdir --ignore-fail-on-non-empty "$WORK_DIR"
fi

echo "All shards have been processed successfully."