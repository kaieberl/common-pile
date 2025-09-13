"""Convert Arxiv Dumps into the dolma format.

When resuming, delete the last (unfinished) shard and set its shard_idx.
"""
import functools
import gzip
import itertools
import json
import logging
import os
import re
import shutil
import tarfile
import tempfile
from typing import Set, Tuple, Iterator, Dict, List

from huggingface_hub import hf_hub_download

from common_pile.write import to_dolma
from parse_arxiv import extract_text_from_latex

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_processed_ids(path: str, filename_pattern: str) -> Set[str]:
    """Scans the output directory for existing shards and returns a set of processed article IDs."""
    processed_ids = set()
    if not os.path.exists(path):
        return processed_ids

    # Create a regex pattern to match shard filenames (e.g., arxiv.0000.jsonl.gz)
    pattern = re.compile(filename_pattern.replace("{}", r"(\d+)"))

    for f in os.listdir(path):
        if pattern.match(f):
            shard_path = os.path.join(path, f)
            logger.info(f"Resuming from shard: Found previous shard {shard_path}")
            with gzip.open(shard_path, "rt", encoding="utf-8") as gf:
                for line in gf:
                    try:
                        # The ID is expected to be in the format "YYMM.XXXXX"
                        processed_ids.add(json.loads(line)["id"].split("/")[-1].split('.')[0])
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Could not parse line in {shard_path}: {e}")
    logger.info(f"Found {len(processed_ids)} previously processed article IDs.")
    return processed_ids


def format_dolma(id, text: str):
    return {
        "id": id,
        "text": text,
        # "source": "arxiv",
        # "added": datetime.datetime.utcnow().isoformat(),
        # "created": article["update_date"],
        # "metadata": {
        #     "license": article["license"],
        #     "url": f"http://arxiv.org/abs/{article['id']}",
        #     "authors": article["authors"],
        #     "title": article["title"],
        # },
    }


def get_article_subdirectories(tar: tarfile.TarFile) -> Dict[str, List[tarfile.TarInfo]]:
    """
    Identifies top-level subdirectories in the tar archive and groups members by them.
    Each top-level subdirectory is considered an "article".
    """
    articles: Dict[str, List[tarfile.TarInfo]] = {}
    for member in tar.getmembers():
        if '/' in member.name:
            article_id = "./" + member.name.split('/')[1]  # always in shape ./article_id/...
            if article_id not in articles:
                articles[article_id] = []
            articles[article_id].append(member)
    return articles


def process_articles_from_gzipped_directory(filepath: str, processed_ids: Set[str]) -> Iterator[Tuple[str, str]]:
    """
    Instead of iterating over multiple .gz files, this function processes a single
    gzipped tar archive which contains subdirectories for each article.
    """

    main_archive_path = hf_hub_download(
        repo_id="kai271/arxiv-papers",
        repo_type="dataset",
        filename=os.path.join("shards", filepath),  # contains Attention is all you need
        # local_dir="models/mobilenet_v2_1.0_224",  # Optional: local destination
        # local_dir_use_symlinks=False  # Optional: copy instead of symlink
    )

    with tarfile.open(main_archive_path, "r:gz") as tar:
        articles = get_article_subdirectories(tar)

        for article_id, members in sorted(articles.items()):
            # Extract the raw article ID (e.g., '0704.0001') to check if it's processed
            raw_article_id = article_id.split('/')[-1]
            if raw_article_id in processed_ids:
                logger.debug(f"Skipping already processed article {raw_article_id}")
                continue

            temp_dir = tempfile.mkdtemp()
            try:
                # Extract all files for the article to a temporary directory
                for member in members:
                    # To handle files in subdirectories within the article directory
                    # we need to preserve the structure.
                    # We strip the top-level directory from the member name.
                    # e.g., '0704.0001/some/file.tex' becomes 'some/file.tex'
                    # The tarfile library extracts to a path, so we join temp_dir and the cleaned member name.
                    # However, the members are already grouped by article, and their names are full paths
                    # like '1706.03762/attention.tex'. We need to extract them into temp_dir.
                    tar.extract(member, path=temp_dir)

                article_path = os.path.join(temp_dir, raw_article_id)
                if not os.path.isdir(article_path):
                     # case where tarball doesn't have the article_id as a directory
                     article_path = temp_dir

                main_tex_files = []
                for root, _, files in os.walk(article_path):
                    for file in files:
                        if file.lower().endswith(".tex"):
                            file_path = os.path.join(root, file)
                            try:
                                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                                    content = f.read()
                                if r"\begin{document}" in content:
                                    main_tex_files.append(file_path)
                            except Exception as e:
                                logger.warning(f"Could not read {file_path}: {e}")

                if not main_tex_files:
                    logger.warning(r"No .tex file containing \begin{document} found for article " + raw_article_id)
                    continue

                # In case of multiple main files, we process the first one found.
                # A better strategy might be needed if this is common.
                main_tex_file = main_tex_files[0]

                try:
                    # Use the parsing function from parse_arxiv.py
                    text = extract_text_from_latex(main_tex_file)
                    yield raw_article_id, text
                except Exception as e:
                    logger.error(f"Failed to process {main_tex_file} for article {raw_article_id}: {e}")

            finally:
                shutil.rmtree(temp_dir)

    os.remove(main_archive_path)


if __name__ == '__main__':
    output_path = "data/arxiv/raw/documents/"
    output_filename = "arxiv.jsonl.gz"
    shard_filename_pattern = "arxiv.{:04d}.jsonl.gz"
    shard_idx = 0

    # Scan for already processed articles to support resuming
    processed_ids = get_processed_ids(output_path, shard_filename_pattern)
    print(f"Found {len(processed_ids)} processed articles, resuming.")

    # TODO: restore shards file
    with open("data/arxiv-shards.txt", "r") as f:
        dirnames = sorted(set(l.strip().split("_")[2] + "_tex.tar.gz" for l in f if l.strip()))

    process_with_resume = functools.partial(process_articles_from_gzipped_directory, processed_ids=processed_ids)
    meta_and_content = itertools.chain(*map(process_with_resume, dirnames))
    dolma = map(lambda x: format_dolma(*x), meta_and_content)
    to_dolma(dolma, "data/arxiv/raw/documents/", "arxiv.jsonl.gz", 1, shard_idx=shard_idx)