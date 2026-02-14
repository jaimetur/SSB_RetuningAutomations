import re
from datetime import date
from math import ceil
from pathlib import Path

from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from pptx import Presentation
from pptx.dml.color import RGBColor
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


def clean_heading_text(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^\d+(?:[.)]|(?:\.\d+)*\.?)[\s]+", "", cleaned)
    return cleaned.strip()


def markdown_segments(text: str) -> list[tuple[str, bool]]:
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


def add_plain_paragraph(doc: Document, text: str, style: str | None = None) -> None:
    paragraph = doc.add_paragraph(style=style) if style else doc.add_paragraph()
    paragraph.add_run(normalize_markdown_inline(text))


def parse_table_row(line: str) -> list[str]:
    stripped = line.strip().strip("|")
    return [cell.strip() for cell in stripped.split("|")]


def is_table_separator(line: str) -> bool:
    stripped = line.strip()
    return bool(re.fullmatch(r"\|?[\s:-]+(\|[\s:-]+)+\|?", stripped))


def enable_toc_update_on_open(doc: Document) -> None:
    settings = doc.settings.element
    node = settings.find(qn("w:updateFields"))
    if node is None:
        node = OxmlElement("w:updateFields")
        settings.append(node)
    node.set(qn("w:val"), "true")


def update_header_date(doc: Document) -> None:
    today = date.today().isoformat()

    def local_name(tag: str) -> str:
        return tag.split("}")[-1]

    for section in doc.sections:
        header_el = section.header._element
        for sdt in header_el.iter():
            if local_name(sdt.tag) != "sdt":
                continue

            is_date_alias = False
            for node in sdt.iter():
                if local_name(node.tag) == "alias" and node.get(qn("w:val")) == "Date":
                    is_date_alias = True
                    break

            if not is_date_alias:
                continue

            for node in sdt.iter():
                if local_name(node.tag) == "t":
                    node.text = today
                    break


def find_template_anchor(doc: Document):
    for paragraph in doc.paragraphs:
        if paragraph.text.strip() == "Heading 1":
            return paragraph
    return None


def remove_from_anchor_to_end(doc: Document, anchor) -> None:
    body = doc._body._element
    removing = False
    for child in list(body):
        if child == anchor._p:
            removing = True
        if removing and child.tag != qn("w:sectPr"):
            body.remove(child)


def build_docx_from_markdown(md_file: Path, docx_file: Path) -> None:
    text = md_file.read_text(encoding="utf-8")
    lines = text.splitlines()

    template_path = first_existing_path(DOCX_TEMPLATE_CANDIDATES)
    doc = Document(str(template_path)) if template_path else Document()

    anchor = find_template_anchor(doc)
    if anchor is not None:
        remove_from_anchor_to_end(doc, anchor)

    i = 0
    while i < len(lines):
        line = lines[i]
        s = line.rstrip()
        if not s:
            doc.add_paragraph("")
            i += 1
            continue

        if s == "---":
            i += 1
            continue

        if s.startswith("| ") and s.endswith(" |") and i + 1 < len(lines) and is_table_separator(lines[i + 1]):
            rows: list[list[str]] = [parse_table_row(s)]
            i += 2
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
            add_plain_paragraph(doc, clean_heading_text(s[2:].strip()), style="Title")
        elif s.startswith("## "):
            add_plain_paragraph(doc, clean_heading_text(s[3:].strip()), style="Heading 1")
        elif s.startswith("### "):
            add_plain_paragraph(doc, clean_heading_text(s[4:].strip()), style="Heading 2")
        elif s.startswith("#### "):
            add_plain_paragraph(doc, clean_heading_text(s[5:].strip()), style="Heading 3")
        elif s.startswith("- "):
            add_markdown_paragraph(doc, s[2:].strip(), style="List Bullet")
        elif re.match(r"^\d+\.\s+", s):
            add_markdown_paragraph(doc, re.sub(r"^\d+\.\s+", "", s), style="List Number")
        else:
            add_markdown_paragraph(doc, s)

        i += 1

    enable_toc_update_on_open(doc)
    update_header_date(doc)
    doc.save(docx_file)


def parse_markdown_sections(md_file: Path) -> list[dict]:
    lines = md_file.read_text(encoding="utf-8").splitlines()
    sections: list[dict] = []
    current = {"title": "Overview", "blocks": []}

    i = 0
    while i < len(lines):
        raw = lines[i].rstrip()
        line = raw.strip()

        if not line or line == "---":
            i += 1
            continue

        if line.startswith("# "):
            i += 1
            continue

        if line.startswith("## "):
            if current["blocks"]:
                sections.append(current)
            current = {"title": clean_heading_text(line[3:].strip()), "blocks": []}
            i += 1
            continue

        if line.startswith("### "):
            current["blocks"].append({"type": "subheading", "text": clean_heading_text(line[4:].strip())})
            i += 1
            continue

        if line.startswith("#### "):
            current["blocks"].append({"type": "subheading", "text": clean_heading_text(line[5:].strip())})
            i += 1
            continue

        if line.startswith("| ") and line.endswith(" |") and i + 1 < len(lines) and is_table_separator(lines[i + 1]):
            rows = [parse_table_row(line)]
            i += 2
            while i < len(lines):
                candidate = lines[i].rstrip()
                if not (candidate.startswith("| ") and candidate.endswith(" |")):
                    break
                rows.append(parse_table_row(candidate.strip()))
                i += 1
            current["blocks"].append({"type": "table", "rows": rows})
            continue

        if line.startswith("- ") or re.match(r"^\d+\.\s+", line):
            items: list[str] = []
            ordered = bool(re.match(r"^\d+\.\s+", line))
            while i < len(lines):
                candidate = lines[i].strip()
                if ordered and re.match(r"^\d+\.\s+", candidate):
                    item = re.sub(r"^\d+\.\s+", "", candidate)
                    items.append(normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", item)))
                    i += 1
                    continue
                if (not ordered) and candidate.startswith("- "):
                    item = candidate[2:].strip()
                    items.append(normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", item)))
                    i += 1
                    continue
                break
            current["blocks"].append({"type": "list", "ordered": ordered, "items": items})
            continue

        paragraph_lines = [normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", line))]
        i += 1
        while i < len(lines):
            nxt = lines[i].strip()
            if not nxt or nxt == "---" or nxt.startswith("## ") or nxt.startswith("### ") or nxt.startswith("#### "):
                break
            if nxt.startswith("| ") or nxt.startswith("- ") or re.match(r"^\d+\.\s+", nxt):
                break
            paragraph_lines.append(normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", nxt)))
            i += 1
        current["blocks"].append({"type": "paragraph", "text": " ".join(paragraph_lines)})

    if current["blocks"]:
        sections.append(current)

    if not sections:
        sections = [{"title": "Overview", "blocks": [{"type": "paragraph", "text": "Refer to the markdown guide for full technical details."}]}]
    return sections


def block_units(block: dict) -> int:
    if block["type"] == "subheading":
        return 2
    if block["type"] == "paragraph":
        return max(2, ceil(len(block["text"]) / 90))
    if block["type"] == "list":
        return 1 + sum(max(1, ceil(len(item) / 95)) for item in block["items"])
    if block["type"] == "table":
        return 5 + len(block["rows"])
    return 2


def style_table(table) -> None:
    header_fill = RGBColor(31, 78, 121)
    band_fill = RGBColor(242, 246, 252)
    text_dark = RGBColor(40, 40, 40)

    for col in range(len(table.columns)):
        cell = table.cell(0, col)
        cell.fill.solid()
        cell.fill.fore_color.rgb = header_fill
        p = cell.text_frame.paragraphs[0]
        p.font.bold = True
        p.font.color.rgb = RGBColor(255, 255, 255)
        p.font.size = Pt(14)

    for row in range(1, len(table.rows)):
        for col in range(len(table.columns)):
            cell = table.cell(row, col)
            if row % 2 == 0:
                cell.fill.solid()
                cell.fill.fore_color.rgb = band_fill
            p = cell.text_frame.paragraphs[0]
            p.font.color.rgb = text_dark
            p.font.size = Pt(12)


def render_text_blocks(tf, blocks: list[dict]) -> None:
    tf.clear()
    first = True
    for block in blocks:
        if block["type"] == "subheading":
            p = tf.paragraphs[0] if first else tf.add_paragraph()
            first = False
            p.text = block["text"]
            p.level = 0
            p.font.bold = True
            p.font.size = Pt(22)
            continue

        if block["type"] == "paragraph":
            p = tf.paragraphs[0] if first else tf.add_paragraph()
            first = False
            p.text = block["text"]
            p.level = 0
            p.font.size = Pt(18)
            continue

        if block["type"] == "list":
            for idx, item in enumerate(block["items"], start=1):
                p = tf.paragraphs[0] if first else tf.add_paragraph()
                first = False
                p.text = f"{idx}. {item}" if block["ordered"] else item
                p.level = 1
                p.font.size = Pt(17)


def add_table_slide(prs: Presentation, title: str, rows: list[list[str]]) -> None:
    slide = prs.slides.add_slide(prs.slide_layouts[1])
    slide.shapes.title.text = title

    content = slide.shapes.placeholders[1]
    x, y, w, h = content.left, content.top, content.width, content.height
    tbl_shape = slide.shapes.add_table(len(rows), len(rows[0]), x, y, w, h)
    table = tbl_shape.table

    for r, row in enumerate(rows):
        for c, value in enumerate(row):
            table.cell(r, c).text = normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", value))

    style_table(table)


def build_pptx_summary(md_file: Path, pptx_file: Path, version: str) -> None:
    sections = parse_markdown_sections(md_file)

    template_path = first_existing_path(PPTX_TEMPLATE_CANDIDATES)
    prs = Presentation(str(template_path)) if template_path else Presentation()

    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "SSB Retuning Automations"
    slide.placeholders[1].text = f"Technical User Guide Summary (v{version})"

    max_units = 20
    for section in sections:
        title = section["title"]
        blocks = section["blocks"]

        text_buffer: list[dict] = []
        used_units = 0

        def flush_text_slide(continuation: bool) -> bool:
            nonlocal text_buffer, used_units
            if not text_buffer:
                return continuation
            slide_local = prs.slides.add_slide(prs.slide_layouts[1])
            slide_local.shapes.title.text = title if not continuation else f"{title} (cont.)"
            render_text_blocks(slide_local.shapes.placeholders[1].text_frame, text_buffer)
            text_buffer = []
            used_units = 0
            return True

        continuation = False
        for block in blocks:
            if block["type"] == "table":
                continuation = flush_text_slide(continuation)
                add_table_slide(prs, title if not continuation else f"{title} (cont.)", block["rows"])
                continuation = True
                continue

            units = block_units(block)
            if text_buffer and used_units + units > max_units:
                continuation = flush_text_slide(continuation)

            text_buffer.append(block)
            used_units += units

        flush_text_slide(continuation)

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
