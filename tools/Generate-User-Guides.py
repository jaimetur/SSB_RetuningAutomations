import re
from pathlib import Path

from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from pptx import Presentation
from pptx.util import Pt

ROOT = Path(__file__).resolve().parents[1]
HELP_DIR = ROOT / "help"
README_PATH = ROOT / "README.md"
TOOL_MAIN_PATH = ROOT / "src" / "SSB_RetuningAutomations.py"
DOCX_TEMPLATE_CANDIDATES = [
    ROOT / "assets" / "docx_templates" / "UserGuideTemplate.docx",
]
PPTX_TEMPLATE_CANDIDATES = [
    ROOT / "assets" / "ppt_templates" / "UserGuideTemplate.pptx",
]


def get_tool_version() -> str:
    text = TOOL_MAIN_PATH.read_text(encoding="utf-8")
    match = re.search(r'^TOOL_VERSION\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)"', text, flags=re.MULTILINE)
    if not match:
        raise RuntimeError("Unable to find TOOL_VERSION in src/SSB_RetuningAutomations.py")
    return match.group(1)


def guide_paths(version: str) -> dict[str, Path]:
    base_name = f"SSB-Retuning-Automations-User-Guide-v{version}"
    return {
        "md": HELP_DIR / f"{base_name}.md",
        "docx": HELP_DIR / f"{base_name}.docx",
        "pptx": HELP_DIR / f"{base_name}.pptx",
    }


def first_existing_path(candidates: list[Path]) -> Path | None:
    for path in candidates:
        if path.exists():
            return path
    return None


def normalize_markdown_inline(text: str) -> str:
    normalized = text.replace("`", '"').replace("'", '"')
    normalized = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1 (\2)", normalized)
    return normalized


def markdown_segments(text: str) -> list[tuple[str, bool]]:
    """Return [(segment, is_bold)] for markdown strings with **bold** markers."""
    clean = normalize_markdown_inline(text)
    parts = re.split(r"(\*\*.*?\*\*)", clean)
    segments: list[tuple[str, bool]] = []
    for part in parts:
        if not part:
            continue
        if part.startswith("**") and part.endswith("**") and len(part) > 4:
            segments.append((part[2:-2], True))
        else:
            segments.append((part, False))
    return segments


def add_markdown_paragraph(doc: Document, text: str, style: str | None = None) -> None:
    paragraph = doc.add_paragraph(style=style) if style else doc.add_paragraph()
    for segment, is_bold in markdown_segments(text):
        run = paragraph.add_run(segment)
        run.bold = is_bold


def parse_table_row(line: str) -> list[str]:
    stripped = line.strip().strip("|")
    return [cell.strip() for cell in stripped.split("|")]


def is_table_separator(line: str) -> bool:
    stripped = line.strip()
    return bool(re.fullmatch(r"\|?[\s:-]+(\|[\s:-]+)+\|?", stripped))


def insert_toc_field(doc: Document) -> None:
    paragraph = doc.add_paragraph()
    run = paragraph.add_run()
    fld_begin = OxmlElement("w:fldChar")
    fld_begin.set(qn("w:fldCharType"), "begin")

    instr_text = OxmlElement("w:instrText")
    instr_text.set(qn("xml:space"), "preserve")
    instr_text.text = 'TOC \\o "1-3" \\h \\z \\u'

    fld_separate = OxmlElement("w:fldChar")
    fld_separate.set(qn("w:fldCharType"), "separate")

    default_text = OxmlElement("w:t")
    default_text.text = "Right-click and update this field to generate the Table of Contents."

    fld_end = OxmlElement("w:fldChar")
    fld_end.set(qn("w:fldCharType"), "end")

    run._r.append(fld_begin)
    run._r.append(instr_text)
    run._r.append(fld_separate)
    run._r.append(default_text)
    run._r.append(fld_end)


def build_docx_from_markdown(md_file: Path, docx_file: Path) -> None:
    text = md_file.read_text(encoding="utf-8")
    lines = text.splitlines()

    template_path = first_existing_path(DOCX_TEMPLATE_CANDIDATES)
    doc = Document(str(template_path)) if template_path else Document()

    i = 0
    while i < len(lines):
        line = lines[i]
        s = line.rstrip()
        if not s:
            doc.add_paragraph("")
            i += 1
            continue

        if s.startswith("| ") and s.endswith(" |") and i + 1 < len(lines) and is_table_separator(lines[i + 1]):
            rows: list[list[str]] = []
            rows.append(parse_table_row(s))
            i += 2  # skip header + separator
            while i < len(lines):
                candidate = lines[i].rstrip()
                if not (candidate.startswith("| ") and candidate.endswith(" |")):
                    break
                rows.append(parse_table_row(candidate))
                i += 1

            if rows:
                table = doc.add_table(rows=len(rows), cols=len(rows[0]))
                table.style = "Table Grid"
                for row_idx, row in enumerate(rows):
                    for col_idx, cell in enumerate(row):
                        paragraph = table.cell(row_idx, col_idx).paragraphs[0]
                        for seg, is_bold in markdown_segments(cell):
                            run = paragraph.add_run(seg)
                            run.bold = is_bold or row_idx == 0
            continue

        if s.startswith("# "):
            add_markdown_paragraph(doc, s[2:].strip(), style="Heading 1")
        elif s.startswith("## "):
            add_markdown_paragraph(doc, s[3:].strip(), style="Heading 2")
        elif s.startswith("### "):
            add_markdown_paragraph(doc, s[4:].strip(), style="Heading 3")
        elif s.startswith("- "):
            add_markdown_paragraph(doc, s[2:].strip(), style="List Bullet")
        else:
            add_markdown_paragraph(doc, s)

        i += 1

    insert_toc_field(doc)

    doc.save(docx_file)


def build_pptx_summary(md_file: Path, pptx_file: Path, version: str) -> None:
    text = md_file.read_text(encoding="utf-8")
    lines = [line.strip() for line in text.splitlines()]

    sections: list[tuple[str, list[str]]] = []
    current_title = "Overview"
    current_bullets: list[str] = []

    for line in lines:
        if line.startswith("## "):
            if current_bullets:
                sections.append((current_title, current_bullets[:6]))
            current_title = line[3:].strip()
            current_bullets = []
        elif line.startswith("- "):
            bullet = line[2:].strip()
            current_bullets.append(normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", bullet)))

    if current_bullets:
        sections.append((current_title, current_bullets[:6]))

    if not sections:
        sections = [("Overview", ["Refer to the markdown guide for full technical details."])]

    template_path = first_existing_path(PPTX_TEMPLATE_CANDIDATES)
    prs = Presentation(str(template_path)) if template_path else Presentation()

    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "SSB Retuning Automations"
    slide.placeholders[1].text = f"Technical User Guide Summary (v{version})"

    for title, bullets in sections[:7]:
        slide = prs.slides.add_slide(prs.slide_layouts[1])
        slide.shapes.title.text = title
        tf = slide.shapes.placeholders[1].text_frame
        tf.clear()
        for i, bullet in enumerate(bullets):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.text = bullet
            p.level = 0
            p.font.size = Pt(20)

    prs.save(pptx_file)


def update_readme_links(version: str) -> None:
    md_name = f"SSB-Retuning-Automations-User-Guide-v{version}.md"
    docx_name = f"SSB-Retuning-Automations-User-Guide-v{version}.docx"
    pptx_name = f"SSB-Retuning-Automations-User-Guide-v{version}.pptx"

    new_block = (
        "## 📙 Technical User Guide\n\n"
        "You can find the technical user guide in these formats:\n"
        f"- [Markdown](help/{md_name})\n"
        f"- [Word](help/{docx_name})\n"
        f"- [PowerPoint](help/{pptx_name})\n"
    )

    readme = README_PATH.read_text(encoding="utf-8")
    pattern = r"## 📙 Technical User Guide\n[\s\S]*?\n---"
    replacement = f"{new_block}\n---"
    if not re.search(pattern, readme):
        raise RuntimeError("Unable to find 'Technical User Guide' section in README.md")

    updated = re.sub(pattern, replacement, readme, count=1)
    README_PATH.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    version = get_tool_version()
    paths = guide_paths(version)

    if not paths["md"].exists():
        raise FileNotFoundError(f"Markdown guide not found: {paths['md']}")

    build_docx_from_markdown(paths["md"], paths["docx"])
    build_pptx_summary(paths["md"], paths["pptx"], version)
    update_readme_links(version)

    print(f"Tool version detected: v{version}")
    print(f"Generated: {paths['docx']}")
    print(f"Generated: {paths['pptx']}")
    print("README technical guide links updated.")
