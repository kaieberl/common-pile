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

    # Define file and directory names for the current shard
    tar_file="${ID}.tar"
    shard_temp_dir="${WORK_DIR}/${ID}_temp"
    mkdir -p "$shard_temp_dir"

    # 1. Download shard from S3
    echo "1. Downloading s3://arxiv/src/$tar_file..."
    aws s3 cp "s3://arxiv/src/$tar_file" "$WORK_DIR/" --request-payer requester

    # 2. Unpack the downloaded tar file
    echo "2. Unpacking $tar_file..."
    tar -xf "$WORK_DIR/$tar_file" -C "$shard_temp_dir"
    rm "$WORK_DIR/$tar_file" # Clean up tar file immediately after extraction

    # 3. Unpack all .gz files and keep only .tex files
    echo "3. Extracting .tex files from .gz archives..."
    find "$shard_temp_dir" -type f -name "*.gz" -print0 | while IFS= read -r -d $'\0' gz_file; do
        # Create a unique name based on the gz_file name
        output_basename=$(basename "${gz_file%.gz}")

        # Unpack and filter for .tex content, or just decompress if it's not a tar archive
        if tar -tzf "$gz_file" &>/dev/null; then
            # It's a .tar.gz. Create a directory for its contents.
            output_dir="${month_dir}/${output_basename}"
            mkdir -p "$output_dir"

            # Extract the archive into the new directory
            tar -xzf "$gz_file" -C "$output_dir"

            # Delete all files that are not .tex files
            find "$output_dir" -type f -not -name "*.tex" -delete

            # Clean up any empty directories left after deleting other files
            find "$output_dir" -depth -type d -empty -delete
        else
            # It's a simple .gz, decompress it to a .tex file
            output_tex_file="${month_dir}/${output_basename}.tex"
            gunzip -c "$gz_file" > "$output_tex_file"

            # If the created file is empty, remove it
            if [ ! -s "$output_tex_file" ]; then
                rm -f "$output_tex_file"
            fi
        fi
    done

    # 4. Clean up the temporary shard directory
    rm -rf "$shard_temp_dir"

    echo "--- Finished Shard ID: $ID ---"
    echo ""

done < <(cat "$ID_FILE"; echo) # Add a newline to process the last month in the file

# Clean up the main working directory if it's empty
if [ -d "$WORK_DIR" ]; then
    rmdir --ignore-fail-on-non-empty "$WORK_DIR"
fi

echo "All shards have been processed successfully."