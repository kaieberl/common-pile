import argparse
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import gzip
from pathlib import Path

# --- Configuration ---
# Configure logging to provide informative output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)


def check_dependencies():
    """Verify that required command-line tools are installed."""
    if not shutil.which("aws"):
        logging.error(
            "AWS CLI ('aws') not found. Please install and configure it. Aborting."
        )
        sys.exit(1)
    if not shutil.which("huggingface-cli"):
        logging.error(
            "Hugging Face CLI ('huggingface-cli') not found. "
            "Please install it ('pip install huggingface_hub'). Aborting."
        )
        sys.exit(1)
    logging.info("All dependencies are satisfied.")


def download_shard_from_s3(shard_id: str, work_dir: Path) -> Path:
    """Downloads a shard from the arXiv S3 bucket."""
    tar_filename = f"{shard_id}.tar"
    s3_uri = f"s3://arxiv/src/{tar_filename}"
    local_path = work_dir / tar_filename

    logging.info(f"Downloading {s3_uri}...")
    try:
        subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                s3_uri,
                str(local_path),
                "--request-payer",
                "requester",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(f"Successfully downloaded {tar_filename}.")
        return local_path
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to download {tar_filename}. Error: {e.stderr}")
        sys.exit(1)


def process_gz_file(gz_path: Path, month_dir: Path):
    """
    Extracts .tex files from a .gz archive.
    The archive can be a single gzipped file or a gzipped tarball (.tar.gz).
    """
    output_basename = gz_path.name.removesuffix(".gz")

    try:
        # Check if the file is a tar archive
        with gzip.open(gz_path, "rb") as f:
            is_tar = f.read(2) == b"\x1f\x8b"  # Check for gzip magic number
        with tarfile.open(gz_path, "r:gz") as tar:
            # It's a .tar.gz file
            output_dir = month_dir / output_basename
            output_dir.mkdir(parents=True, exist_ok=True)

            # Extract only .tex files
            tex_members = [m for m in tar.getmembers() if m.name.endswith(".tex") and m.isfile()]
            for member in tex_members:
                tar.extract(member, path=output_dir)

            logging.info(f"Extracted .tex files to {output_dir}")

            # If no .tex files were found, clean up the created directory
            if not any(output_dir.iterdir()):
                output_dir.rmdir()

    except tarfile.ReadError:
        # It's a simple .gz file, not a tarball
        output_tex_file = month_dir / f"{output_basename}.tex"
        logging.info(f"Decompressing {gz_path.name} to {output_tex_file.name}...")
        try:
            with gzip.open(gz_path, "rb") as f_in, open(output_tex_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            # Remove empty files
            if output_tex_file.stat().st_size == 0:
                output_tex_file.unlink()
                logging.info(f"Removed empty file: {output_tex_file.name}")

        except Exception as e:
            logging.warning(f"Could not decompress {gz_path.name}: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {gz_path.name}: {e}")


def process_shard(shard_id: str, work_dir: Path, month_dir: Path):
    """
    Handles the full processing for a single shard: download, unpack,
    and extract .tex files.
    """
    logging.info(f"--- Processing Shard ID: {shard_id} ---")

    # 1. Download shard
    tar_path = download_shard_from_s3(shard_id, work_dir)
    if not tar_path:
        return

    # 2. Unpack the main shard tar file
    shard_temp_dir = work_dir / f"{shard_id}_temp"
    shard_temp_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Unpacking {tar_path.name}...")
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=shard_temp_dir)
    tar_path.unlink()  # Clean up tar file immediately

    # 3. Find and process all .gz files
    logging.info("Extracting .tex files from .gz archives...")
    gz_files = list(shard_temp_dir.rglob("*.gz"))
    for gz_file in gz_files:
        process_gz_file(gz_file, month_dir)

    # 4. Clean up the temporary shard directory
    shutil.rmtree(shard_temp_dir)
    logging.info(f"--- Finished Shard ID: {shard_id} ---")


def package_and_upload_month(month: str, month_dir: Path, hf_repo: str):
    """
    Packages all .tex files for a given month into a tar.gz archive
    and uploads it to Hugging Face.
    """
    logging.info(f"--- Finished processing shards for month: {month} ---")

    if not any(month_dir.iterdir()):
        logging.warning(f"No .tex files found for month {month}. Skipping packaging and upload.")
        return

    # 1. Pack the month's .tex files
    output_file = month_dir.parent / f"{month}_tex.tar.gz"
    logging.info(f"1. Packing .tex files into {output_file.name}...")
    with tarfile.open(output_file, "w:gz") as tar:
        tar.add(month_dir, arcname=os.path.basename(month_dir))

    # 2. Upload to Hugging Face
    hf_path = f"shards/{output_file.name}"
    logging.info(f"2. Uploading {output_file.name} to HF repo: {hf_repo}")
    try:
        subprocess.run(
            [
                "huggingface-cli",
                "upload",
                hf_repo,
                str(output_file),
                hf_path,
                "--repo-type",
                "dataset",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Failed to upload {output_file.name} to Hugging Face. "
            f"Error: {e.stderr}"
        )
        output_file.unlink()  # Clean up before exiting
        sys.exit(1)

    # 3. Clean up the month's archive and directory
    logging.info("3. Cleaning up local files...")
    output_file.unlink()
    shutil.rmtree(month_dir)


def main():
    """Main function to orchestrate the shard processing."""
    parser = argparse.ArgumentParser(
        description="Processes arXiv source files based on a list of shard IDs.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "id_file",
        nargs="?",
        default="arxiv-shards.txt",
        help="Path to the file containing shard IDs, one per line.\nDefaults to 'arxiv-shards.txt'.",
    )
    parser.add_argument(
        "--repo",
        default="kai271/arxiv-papers",
        help="Hugging Face repository to upload to (e.g., 'user/repo')."
    )
    parser.add_argument(
        "--work-dir",
        default="data",
        help="Local working directory for downloads and temporary files."
    )
    args = parser.parse_args()

    check_dependencies()

    work_dir = Path(args.work_dir)
    work_dir.mkdir(exist_ok=True)

    try:
        with open(args.id_file, "r") as f:
            shard_ids = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logging.error(f"ID file not found: {args.id_file}. Aborting.")
        sys.exit(1)

    current_month = None
    month_dir = None

    for shard_id in shard_ids:
        try:
            shard_month = shard_id.split("_")[2]
        except IndexError:
            logging.warning(f"Could not parse month from shard ID: '{shard_id}'. Skipping.")
            continue

        if shard_month != current_month:
            # If we are starting a new month, process the previous one first
            if current_month and month_dir:
                package_and_upload_month(current_month, month_dir, args.repo)

            # Initialize for the new month
            logging.info(f"--- Starting new month: {shard_month} ---")
            current_month = shard_month
            month_dir = work_dir / current_month
            month_dir.mkdir(parents=True, exist_ok=True)

        process_shard(shard_id, work_dir, month_dir)

    # Process the very last month after the loop finishes
    if current_month and month_dir:
        package_and_upload_month(current_month, month_dir, args.repo)

    # Clean up the main working directory if it's empty
    try:
        work_dir.rmdir()
        logging.info(f"Cleaned up empty working directory: {work_dir}")
    except OSError:
        # Directory is not empty, which is fine
        pass

    logging.info("All shards have been processed successfully.")


if __name__ == "__main__":
    main()