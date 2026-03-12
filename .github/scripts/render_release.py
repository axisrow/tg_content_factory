#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render templated GitHub release notes.")
    parser.add_argument("--repo", required=True, help="GitHub repository in OWNER/REPO format.")
    parser.add_argument("--tag", required=True, help="Release tag, for example v0.1.10.")
    parser.add_argument("--title-file", required=True, help="Path to write the rendered release title.")
    parser.add_argument("--notes-file", required=True, help="Path to write the rendered release notes.")
    parser.add_argument(
        "--metadata-dir",
        default=".github/release-metadata",
        help="Directory containing per-tag release metadata JSON files.",
    )
    return parser.parse_args()


def load_metadata(tag: str, metadata_dir: Path) -> dict:
    metadata_path = metadata_dir / f"{tag}.json"
    if not metadata_path.exists():
        raise SystemExit(
            f"Missing release metadata: {metadata_path}. "
            "Add a JSON file for this tag before pushing the release tag."
        )

    metadata = json.loads(metadata_path.read_text())
    metadata_tag = metadata.get("tag")
    if metadata_tag != tag:
        raise SystemExit(
            f"Release metadata tag mismatch in {metadata_path}: expected {tag}, got {metadata_tag!r}."
        )

    if not metadata.get("title"):
        raise SystemExit(f"Release metadata {metadata_path} must define a non-empty 'title'.")

    highlights = metadata.get("highlights")
    if not isinstance(highlights, list) or not highlights:
        raise SystemExit(f"Release metadata {metadata_path} must define a non-empty 'highlights' list.")

    return metadata


def generate_notes(repo: str, tag: str) -> dict:
    result = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repo}/releases/generate-notes",
            "--method",
            "POST",
            "-f",
            f"tag_name={tag}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def strip_generator_comment(body: str) -> str:
    stripped = body.lstrip()
    if stripped.startswith("<!--"):
        end = stripped.find("-->")
        if end != -1:
            stripped = stripped[end + 3 :]
    return stripped.lstrip()


def normalize_single_category(body: str) -> str:
    marker = "\n**Full Changelog**:"
    head, sep, tail = body.partition(marker)
    headings = head.count("\n### ")
    prefix = "## What's Changed\n### "
    if headings == 1 and head.startswith(prefix):
        _, remainder = head.split("\n", 1)
        head = "## What's Changed\n" + remainder.split("\n", 1)[1]
    if not sep:
        return head
    return head + sep + tail


def render_highlights(highlights: list[dict]) -> str:
    lines = ["### Highlights", ""]
    for item in highlights:
        title = str(item.get("title", "")).strip()
        description = str(item.get("description", "")).strip()
        if not title or not description:
            raise SystemExit("Each highlight item must include non-empty 'title' and 'description'.")
        lines.append(f"- **{title}** — {description}")
    return "\n".join(lines)


def inject_highlights(body: str, highlights_block: str) -> str:
    marker = "\n**Full Changelog**:"
    if marker in body:
        head, tail = body.split(marker, 1)
        head = head.rstrip()
        return f"{head}\n\n{highlights_block}\n\n**Full Changelog**:{tail}"
    return f"{body.rstrip()}\n\n{highlights_block}\n"


def write_text(path: str, content: str) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content.rstrip() + "\n")


def main() -> int:
    args = parse_args()
    metadata = load_metadata(args.tag, Path(args.metadata_dir))
    generated = generate_notes(args.repo, args.tag)
    generated_body = normalize_single_category(strip_generator_comment(str(generated.get("body", ""))))
    highlights_block = render_highlights(metadata["highlights"])
    rendered_body = inject_highlights(generated_body, highlights_block)
    rendered_title = f"{args.tag} — {metadata['title']}"

    write_text(args.title_file, rendered_title)
    write_text(args.notes_file, rendered_body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
