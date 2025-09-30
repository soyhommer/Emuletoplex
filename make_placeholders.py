import argparse
from pathlib import Path
import sys
import yaml

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".wmv", ".mpg", ".mpeg", ".ts", ".m2ts", ".flv"}

def load_config():
    # Tries config.yaml first, then config.example.yaml
    for name in ("config.yaml", "config.example.yaml"):
        p = Path(name)
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
    return {}

def sanitize_segment(seg: str) -> str:
    # Make Windows path-friendly from things like "D:\eMule\Incoming"
    bad = '<>:"/\\|?*'
    seg = seg.replace(":", "_")
    for ch in bad:
        seg = seg.replace(ch, "_")
    return "_".join(seg.split())

def iter_videos(root: Path, recursive: bool):
    pattern = "**/*" if recursive else "*"
    for p in root.glob(pattern):
        if p.is_file() and p.suffix.lower() in VIDEO_EXTS:
            yield p

def main():
    ap = argparse.ArgumentParser(description="Generate empty .txt placeholders from video files")
    ap.add_argument("--source", action="append", help="Source folder with downloaded videos (can be repeated).")
    ap.add_argument("--out", default="./test_placeholders", help="Output folder for .txt placeholders (default: ./test_placeholders)")
    ap.add_argument("--recursive", action="store_true", help="Recurse into subfolders")
    ap.add_argument("--use-config", action="store_true",
                    help="Load emule.watch_paths from config.yaml and use them as sources (in addition to any --source).")
    args = ap.parse_args()

    sources = []
    if args.use_config:
        cfg = load_config()
        emule = (cfg or {}).get("emule") or {}
        watch_paths = emule.get("watch_paths") or []
        sources.extend(watch_paths)

    if args.source:
        sources.extend(args.source)

    # Deduplicate while preserving order
    seen = set()
    uniq_sources = []
    for s in sources:
        if s and s not in seen:
            seen.add(s)
            uniq_sources.append(s)

    if not uniq_sources:
        print("No sources provided. Use --source FOLDER or --use-config.", file=sys.stderr)
        sys.exit(2)

    out_root = Path(args.out).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    multi = len(uniq_sources) > 1
    total = 0
    for src in uniq_sources:
        srcp = Path(src)
        if not srcp.exists():
            print(f"[WARN] Source not found: {srcp}", file=sys.stderr)
            continue

        # If multiple sources, put each set of placeholders under a subfolder named after the source
        target_base = out_root / (sanitize_segment(srcp.name) if not srcp.drive else sanitize_segment(str(srcp)))
        if not multi:
            target_base = out_root

        target_base.mkdir(parents=True, exist_ok=True)
        count = 0
        for vid in iter_videos(srcp, args.recursive):
            placeholder = target_base / (vid.stem + ".txt")
            if not placeholder.exists():
                placeholder.touch()
            count += 1
            total += 1
        print(f"[OK] {count} placeholders from {srcp} -> {target_base}")

    print(f"Done. {total} placeholder .txt files ensured in {out_root}")

if __name__ == "__main__":
    main()