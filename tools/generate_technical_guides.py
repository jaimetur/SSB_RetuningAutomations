import re
from pathlib import Path

from docx import Document
from pptx import Presentation
from pptx.util import Pt

ROOT = Path(__file__).resolve().parents[1]
HELP_DIR = ROOT / "help"
README_PATH = ROOT / "README.md"
TOOL_MAIN_PATH = ROOT / "src" / "SSB_RetuningAutomations.py"


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


def build_docx_from_markdown(md_file: Path, docx_file: Path) -> None:
    text = md_file.read_text(encoding="utf-8")
    lines = text.splitlines()

    doc = Document()
    for line in lines:
        s = line.rstrip()
        if not s:
            doc.add_paragraph("")
            continue
        if s.startswith("# "):
            doc.add_heading(s[2:].strip(), level=1)
        elif s.startswith("## "):
            doc.add_heading(s[3:].strip(), level=2)
        elif s.startswith("### "):
            doc.add_heading(s[4:].strip(), level=3)
        elif s.startswith("- "):
            doc.add_paragraph(s[2:].strip(), style="List Bullet")
        elif s.startswith("| ") and s.endswith(" |"):
            doc.add_paragraph(s)
        else:
            doc.add_paragraph(s)

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
            current_bullets.append(line[2:].strip())

    if current_bullets:
        sections.append((current_title, current_bullets[:6]))

    if not sections:
        sections = [("Overview", ["Refer to the markdown guide for full technical details."])]

    prs = Presentation()

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
