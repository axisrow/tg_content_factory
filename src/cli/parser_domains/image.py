from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    # ── image ──
    image_parser = subparsers.add_parser("image", help="Image generation")
    image_sub = image_parser.add_subparsers(dest="image_action")

    image_gen = image_sub.add_parser("generate", help="Generate an image from prompt")
    image_gen.add_argument("prompt", help="Text prompt for image generation")
    image_gen.add_argument("--model", default=None, help="Model string (e.g. replicate:flux-schnell)")

    image_models = image_sub.add_parser("models", help="Search available models")
    image_models.add_argument("--provider", required=True, help="Provider name (replicate, together, openai)")
    image_models.add_argument("--query", default="", help="Search query")

    image_sub.add_parser("providers", help="List configured image providers")

    image_generated = image_sub.add_parser("generated", help="List generated images")
    image_generated.add_argument("--limit", type=int, default=20, help="Max images to show")
