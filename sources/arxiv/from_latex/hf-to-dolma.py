"""Convert Arxiv Dumps into the dolma format."""
import itertools
import logging
import os
import re
import tarfile
from typing import Set, Tuple, Iterator, Dict, List

from charset_normalizer import from_bytes
from huggingface_hub import hf_hub_download

from common_pile.write import to_dolma

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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


def skip_file(filename: str, to_skip: Set[str]) -> bool:
    r"""Check if filename.ext or just filename is in to_skip.

    Note:
      Sometimes people use \input{filename} or \input{filename.tex}
      to include other files in latex.
    """
    return filename in to_skip or os.path.splitext(filename)[0] in to_skip


def read_file(tar, file_info, article_id: str):
    try:
        contents = tar.extractfile(file_info).read()
        if isinstance(contents, bytes):
            contents = str(from_bytes(contents).best())
        return contents
    except Exception as e:
        logger.warning(
            f"Failed to read {file_info.filename} for article [{article_id}]: {e}"
        )
        return ""


def interpolate_document(
        contents: str, tar: tarfile.TarFile, skip: Set[str], article_id: str
) -> Tuple[str, Set[str]]:
    """
    Recursively interpolates content from `\input{}` commands in a LaTeX file.
    Modified to handle paths within article-specific subdirectories.
    """
    full_contents = []
    offset = 0
    pattern = r"^[^\S\n]*[^\S%]*?(?P<cmd>\\input{(?P<filename>.*?)})"

    # Capture the whole command so we know the bounds and can remove it.
    for m in re.finditer(pattern, contents, re.MULTILINE):
        # Add everything before the \input command.
        full_contents.append(contents[offset: m.start()])
        offset = m.end()

        try:
            # --- MODIFICATION START ---
            # Construct the full path to the input file within the tar archive.
            # The path is relative to the article's subdirectory (article_id).
            # This handles cases like `\input{chapter1}` and `\input{chapter1.tex}`.
            base_filename, _ = os.path.splitext(m.group('filename'))
            input_file_path = os.path.join(article_id, f"{base_filename}.tex")
            # --- MODIFICATION END ---

            input_file_info = tar.getmember(input_file_path)
            input_contents = read_file(tar, input_file_info, article_id)
            logger.debug(f"Interpolating {input_file_path} into a document for {article_id}")
            full_contents.append(input_contents)

            # Track which files we have interpolated to avoid using them as top-level documents.
            # We add the full path (e.g., 'article1/chapter1.tex') to the skip set.
            skip.add(input_file_info.name)

        except Exception as e:
            logger.warning(
                f"Failed to interpolate {m.group('filename')} while processing [{article_id}]: {e}"
            )
            # If we fail, just put the raw \input command back.
            full_contents.append(m.group('cmd'))

    # Include everything from the end of the final match until the end of the string.
    full_contents.append(contents[offset:])
    return "".join(full_contents), skip


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


def process_articles_from_gzipped_directory(filepath: str) -> Iterator[Tuple[str, str]]:
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

        for article_id, members in articles.items():
            skip = set()
            for info in members:
                # If we interpolated this file into another, don't use it as a document.
                if skip_file(info.name, skip):
                    continue

                # The path of the file inside the article subdirectory.
                relative_path = os.path.relpath(info.name, article_id)

                # If the file is a .tex document at the root of the article subdirectory.
                if (
                        os.path.splitext(relative_path)[1].lower() == ".tex"
                        and os.path.dirname(relative_path) == ""
                ):
                    logger.debug(
                        f"Creating a document from {article_id}/{relative_path}"
                    )
                    contents = read_file(tar, info, article_id)
                    # Only output files that include \begin{document}.
                    if r"\begin{document}" in contents:
                        content, skip = interpolate_document(
                            contents, tar, skip, article_id
                        )
                        yield info.name.split("/")[1], content
                elif (
                        os.path.splitext(info.name)[1] == ".tex"
                        and relative_path == "."
                ):
                    logger.debug(
                        f"Creating a document from {article_id}/{relative_path}"
                    )
                    contents = read_file(tar, info, article_id)
                    # Only output files that include \begin{document}.
                    if r"\begin{document}" in contents:
                        content, skip = interpolate_document(
                            contents, tar, skip, article_id
                        )
                        yield info.name.split("/")[1][:-4], content


if __name__ == '__main__':
    # TODO: restore shards file
    with open("data/arxiv-shards.txt", "r") as f:
        dirnames = set("data/" + l.split("_")[2] + "_tex.tar.gz" for l in f)

    meta_and_content = itertools.chain(*map(process_articles_from_gzipped_directory, dirnames))
    dolma = map(lambda x: format_dolma(*x), meta_and_content)
    to_dolma(dolma, "data/arxiv/raw/documents/", "arxiv.jsonl.gz", 1)
