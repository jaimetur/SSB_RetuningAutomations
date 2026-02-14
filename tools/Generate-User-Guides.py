import re
from datetime import datetime
from pathlib import Path

from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
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
    """Remove manual markdown numbering so templates can apply automatic numbering."""
    cleaned = text.strip()
    # e.g. "1) Title", "2.3 Subtitle", "4.1.2 Topic"
    cleaned = re.sub(r"^\d+(?:[.)]|(?:\.\d+)*\.?)[\s]+", "", cleaned)
    return cleaned.strip()


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


def add_plain_paragraph(doc: Document, text: str, style: str | None = None) -> None:
    paragraph = doc.add_paragraph(style=style) if style else doc.add_paragraph()
    paragraph.add_run(normalize_markdown_inline(text))


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


def delete_paragraph(paragraph) -> None:
    p = paragraph._element
    p.getparent().remove(p)


def find_content_anchor_index(doc: Document) -> int:
    """Find where generated content must start (template says 'Heading 1' on page 4)."""
    for idx, paragraph in enumerate(doc.paragraphs):
        text = paragraph.text.strip().lower()
        style = (paragraph.style.name or "").strip().lower() if paragraph.style else ""
        if text == "heading 1" and style == "heading 1":
            return idx
    raise RuntimeError("Template content anchor 'Heading 1' (style Heading 1) not found.")


def truncate_document_from_anchor(doc: Document, anchor_index: int) -> None:
    """Keep template pages intact and clear body only from the anchor onwards."""
    for paragraph in list(doc.paragraphs[anchor_index:]):
        delete_paragraph(paragraph)


def mark_toc_fields_dirty(doc: Document) -> None:
    """Force Word to refresh TOC fields when opening the generated document."""
    for fld in doc._element.xpath(".//w:fldSimple"):
        instr = (fld.get(qn("w:instr")) or "").upper()
        if "TOC" in instr:
            fld.set(qn("w:dirty"), "true")

    for instr in doc._element.xpath(".//w:instrText"):
        text = (instr.text or "").upper()
        if "TOC" in text:
            parent = instr.getparent()
            if parent is not None:
                parent.set(qn("w:dirty"), "true")


def update_header_generation_date(doc: Document) -> None:
    """Update any header content controls tagged as Date with today's date."""

    def local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    today = datetime.now().strftime("%Y-%m-%d")
    for section in doc.sections:
        for header in [section.header, section.first_page_header, section.even_page_header]:
            for sdt in header._element.iter():
                if local_name(sdt.tag) != "sdt":
                    continue

                metadata_chunks: list[str] = []
                text_nodes = []
                for node in sdt.iter():
                    name = local_name(node.tag)
                    if name in {"alias", "tag"}:
                        for attr_key, attr_val in node.attrib.items():
                            if local_name(attr_key) == "val":
                                metadata_chunks.append(attr_val)
                    if name == "t":
                        text_nodes.append(node)

                if "date" not in " ".join(metadata_chunks).lower():
                    continue
                if text_nodes:
                    text_nodes[0].text = today
                    for extra in text_nodes[1:]:
                        extra.text = ""


def build_docx_from_markdown(md_file: Path, docx_file: Path) -> None:
    text = md_file.read_text(encoding="utf-8")
    lines = text.splitlines()

    template_path = first_existing_path(DOCX_TEMPLATE_CANDIDATES)
    doc = Document(str(template_path)) if template_path else Document()

    if template_path:
        anchor_index = find_content_anchor_index(doc)
        truncate_document_from_anchor(doc, anchor_index)

    title_written = False

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
            # Markdown H1 is the document title (not part of heading numbering hierarchy).
            add_plain_paragraph(doc, clean_heading_text(s[2:].strip()), style="Title")
            title_written = True
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

    if title_written:
        doc.add_paragraph("")

    mark_toc_fields_dirty(doc)
    update_header_generation_date(doc)

    doc.save(docx_file)


def estimate_point_weight(point: dict) -> int:
    text = point["text"]
    if point["kind"] == "table":
        rows = len(point["rows"])
        cols = len(point["rows"][0]) if point["rows"] else 0
        return 160 + rows * cols * 12
    return max(40, 30 + len(text))


def parse_markdown_sections(md_file: Path) -> list[tuple[str, list[dict]]]:
    text = md_file.read_text(encoding="utf-8")
    lines = [line.rstrip() for line in text.splitlines()]

    sections: list[tuple[str, list[dict]]] = []
    current_title = "Overview"
    current_points: list[dict] = []
    group_id = 0

    def flush_section() -> None:
        if current_points:
            sections.append((current_title, current_points.copy()))

    i = 0
    while i < len(lines):
        raw_line = lines[i]
        line = raw_line.strip()
        if not line or line == "---":
            i += 1
            continue

        if line.startswith("# "):
            i += 1
            continue

        if line.startswith("## "):
            flush_section()
            current_title = clean_heading_text(line[3:].strip())
            current_points = []
            group_id += 1
            i += 1
            continue

        if line.startswith("| ") and line.endswith(" |") and i + 1 < len(lines) and is_table_separator(lines[i + 1]):
            rows: list[list[str]] = [parse_table_row(line)]
            i += 2
            while i < len(lines):
                candidate = lines[i].strip()
                if not (candidate.startswith("| ") and candidate.endswith(" |")):
                    break
                rows.append(parse_table_row(candidate))
                i += 1
            current_points.append({"kind": "table", "rows": rows, "text": "", "level": 0, "group": group_id})
            group_id += 1
            continue

        if line.startswith("### "):
            current_points.append({"kind": "text", "text": clean_heading_text(line[4:].strip()), "level": 0, "group": group_id})
            group_id += 1
            i += 1
            continue

        if line.startswith("#### "):
            current_points.append({"kind": "text", "text": clean_heading_text(line[5:].strip()), "level": 1, "group": group_id})
            i += 1
            continue

        if line.startswith("- "):
            bullet = normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", line[2:].strip()))
            current_points.append({"kind": "text", "text": bullet, "level": 1, "group": group_id})
            i += 1
            continue

        numbered_match = re.match(r"^(\d+)\.\s+(.*)", line)
        if numbered_match:
            current_points.append({"kind": "text", "text": normalize_markdown_inline(numbered_match.group(2).strip()), "level": 1, "group": group_id})
            i += 1
            continue

        current_points.append({"kind": "text", "text": normalize_markdown_inline(re.sub(r"\*\*(.*?)\*\*", r"\1", line)), "level": 0, "group": group_id})
        group_id += 1
        i += 1

    flush_section()
    return sections


def paginate_points(points: list[dict], capacity: int = 900) -> list[list[dict]]:
    if not points:
        return [[{"kind": "text", "text": "Refer to the markdown guide for full details.", "level": 0, "group": 0}]]

    slides: list[list[dict]] = []
    current: list[dict] = []
    used = 0
    idx = 0
    while idx < len(points):
        point = points[idx]
        weight = estimate_point_weight(point)
        group = [point]
        j = idx + 1
        while j < len(points) and points[j]["group"] == point["group"]:
            group.append(points[j])
            j += 1
        group_weight = sum(estimate_point_weight(item) for item in group)

        if current and used + group_weight > capacity:
            slides.append(current)
            current = []
            used = 0

        if group_weight <= capacity:
            current.extend(group)
            used += group_weight
            idx = j
            continue

        if current:
            slides.append(current)
            current = []
            used = 0
        current.append(point)
        used += weight
        idx += 1

    if current:
        slides.append(current)

    return slides


def build_pptx_summary(md_file: Path, pptx_file: Path, version: str) -> None:
    sections = parse_markdown_sections(md_file)

    if not sections:
        sections = [("Overview", [{"kind": "text", "text": "Refer to the markdown guide for full technical details.", "level": 0, "group": 0}])]

    template_path = first_existing_path(PPTX_TEMPLATE_CANDIDATES)
    prs = Presentation(str(template_path)) if template_path else Presentation()

    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "SSB Retuning Automations"
    slide.placeholders[1].text = f"Technical User Guide Summary (v{version})"

    for title, points in sections:
        chunks = paginate_points(points)
        for idx, chunk in enumerate(chunks):
            slide = prs.slides.add_slide(prs.slide_layouts[1])
            slide.shapes.title.text = title if idx == 0 else f"{title} (cont.)"
            tf = slide.shapes.placeholders[1].text_frame
            tf.clear()

            first_text_written = False
            for point in chunk:
                if point["kind"] == "table":
                    left, top = slide.shapes.placeholders[1].left, slide.shapes.placeholders[1].top
                    width, height = slide.shapes.placeholders[1].width, slide.shapes.placeholders[1].height
                    rows = len(point["rows"])
                    cols = len(point["rows"][0]) if point["rows"] else 0
                    if rows and cols:
                        table_shape = slide.shapes.add_table(rows, cols, left, top, width, height)
                        table = table_shape.table
                        for r in range(rows):
                            for c in range(cols):
                                cell = table.cell(r, c)
                                cell.text = normalize_markdown_inline(point["rows"][r][c])
                                para = cell.text_frame.paragraphs[0]
                                para.alignment = PP_ALIGN.LEFT
                                para.font.size = Pt(14 if r == 0 else 12)
                                if r == 0:
                                    para.font.bold = True
                                    cell.fill.solid()
                                    cell.fill.fore_color.rgb = RGBColor(230, 235, 245)
                    continue

                p = tf.paragraphs[0] if not first_text_written else tf.add_paragraph()
                p.text = point["text"]
                p.level = point["level"]
                p.font.size = Pt(18 if point["level"] == 0 else 16)
                first_text_written = True

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
