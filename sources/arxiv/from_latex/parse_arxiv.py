import pylatexenc.latex2text
import os
import re
from charset_normalizer import from_bytes
from pylatexenc.macrospec import MacroSpec, ParsedMacroArgs

# Custom handler for the \href macro to prevent crashes
def href_simplify_repl(node: ParsedMacroArgs, l2tobj: pylatexenc.latex2text.LatexNodes2Text):
    """
    A robust replacement for the \href macro.
    Handles cases where \href has one or two arguments.
    """
    if node.nodeargd is None or not node.nodeargd.argnlist:
        return ""

    # \href{url}{text}
    if len(node.nodeargd.argnlist) >= 2:
        url = l2tobj.nodelist_to_text(node.nodeargd.argnlist[0].nodelist)
        text = l2tobj.nodelist_to_text(node.nodeargd.argnlist[1].nodelist)
        return f'{text} <{url}>'

    # \href{url}
    if len(node.nodeargd.argnlist) == 1:
        url = l2tobj.nodelist_to_text(node.nodeargd.argnlist[0].nodelist)
        return f'<{url}>'

    return ""


l2t_db = pylatexenc.latex2text.get_default_latex_context_db()
l2t_db.add_context_category(
    'overrides',
    prepend=True,
    macros=[
        pylatexenc.latex2text.MacroTextSpec('includegraphics'),
        pylatexenc.latex2text.MacroTextSpec('maketitle'),
        # Add our robust \href handler
        pylatexenc.latex2text.MacroTextSpec('href', simplify_repl=href_simplify_repl),
    ],
    environments=[
        pylatexenc.latex2text.EnvironmentTextSpec('array'),
        pylatexenc.latex2text.EnvironmentTextSpec('pmatrix'),
        pylatexenc.latex2text.EnvironmentTextSpec('bmatrix'),
        pylatexenc.latex2text.EnvironmentTextSpec('smallmatrix'),
    ]
)


def read_file_content(filepath: str) -> str:
    """Reads a file with robust encoding detection."""
    try:
        with open(filepath, 'rb') as f:
            raw_content = f.read()

        if not raw_content:
            return ""

        return str(from_bytes(raw_content).best())
    except FileNotFoundError:
        return ""
    except Exception:
        # Fallback for any other reading errors
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception:
            return ""


def replace_inputs(latex: str, base_dir: str):
    def replace_input(match):
        filename = match.group(1)
        if not filename.endswith('.tex'):
            filename += '.tex'

        filepath = os.path.join(base_dir, filename)
        if os.path.exists(filepath):
            return read_file_content(filepath)
        else:
            return match.group(0)

    input_pattern = r'\\input\{([a-zA-Z0-9_-]+)\}'
    return re.sub(input_pattern, replace_input, latex)


def parse_citations(latex):
    bibitem_pattern = r'\\bibitem(?:\[([^\]]+)\])?\{([^}]+)\}'
    try:
        matches = re.findall(bibitem_pattern, latex)
    except Exception:
        matches = []
    bibitems = {key: label if label else None for label, key in matches}

    for n, (key, label) in enumerate(bibitems.items()):
        if label is None:
            bibitems[key] = str(n + 1)
        else:
            label = " ".join([l.strip() for l in label.splitlines()]).strip("{}")
            label_matches = re.search(r"(.+)\((\d{4})\)?(.+)?", label)
            if label_matches:
                authors, year = label_matches.group(1, 2)
                if authors:
                    year = year or "n.d."
                    bibitems[key] = f"{authors.strip()}, {year}"
                else:
                    bibitems[key] = str(n + 1)
            else:
                bibitems[key] = str(n + 1)

    def replace_cite(match):
        cite_type = match.group(1)
        keys = [key.strip() for key in match.group(2).split(',')]
        if not all([key in bibitems for key in keys]):
            return match.group(0)

        citations = [bibitems[key] for key in keys]

        if cite_type in ['citet', 'citealt', 'citealp']:
            return ', '.join(citations)
        else:
            return f"[{', '.join(citations)}]"

    cite_pattern = r'\\(cite[a-z]*)\{([^}]+)\}'
    latex = re.sub(cite_pattern, replace_cite, latex)
    return re.sub(bibitem_pattern, lambda match: f"[{bibitems.get(match.group(2), match.group(2))}]", latex)


def extract_text_from_latex(latex_filename):
    base_dir = os.path.dirname(latex_filename)
    latex = read_file_content(latex_filename)

    bbl_filename = os.path.splitext(latex_filename)[0] + ".bbl"
    if os.path.exists(bbl_filename):
        bbl_content = read_file_content(bbl_filename)
        bib_pattern = r"(\\bibliography\{[a-zA-Z0-9_-]+\}|\\printbibliography)"
        latex = re.sub(bib_pattern, lambda _: bbl_content, latex)

    if r"\begin{document}" in latex:
        latex = latex.split(r"\begin{document}")[1]

    latex = replace_inputs(latex, base_dir)
    latex = parse_citations(latex)

    text = pylatexenc.latex2text.LatexNodes2Text(math_mode="verbatim", latex_context=l2t_db).latex_to_text(latex)

    text = "\n".join([l.strip() for l in text.splitlines()])
    text = "\n".join(
        [
            l for l in text.splitlines()
            if len(l.split()) > 1
               or l == ""
               or l.startswith("\\")
               or re.match(r"^\[[0-9]+\]$", l)
        ]
    )
    text = re.sub("\n\n+", "\n\n", text)
    return text