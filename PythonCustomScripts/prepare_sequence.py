#!/usr/bin/env python3
"""
prepare_sequence.py

Given an input directory of uniquely indexed image frames (e.g. 00000001.png to 00000281.png)
that may have large gaps in their numbering, produce a new output directory containing a
fully sequential, evenly spaced image sequence:

- Small gaps (<= threshold) are ignored (frames treated as consecutive).
- Large gaps (> threshold) are filled by duplicating the previous frame as many times
  as needed to cover the gap.
- All files are renamed to a contiguous zero-padded sequence starting at 00000001.png.

Usage:
    python prepare_sequence.py \
        --input-dir /path/to/deduped_frames \
        --output-dir /path/to/prepared_sequence \
        [--threshold 5]
"""

import os
import sys
import argparse
import shutil

def parse_args():
    p = argparse.ArgumentParser(description="Prepare image sequence for Flowframes interpolation.")
    p.add_argument("--input-dir",  required=True, help="Folder containing deduplicated frames")
    p.add_argument("--output-dir", required=True, help="Folder to write the new, gap-free sequence")
    p.add_argument(
        "--threshold", "-t",
        type=int,
        default=5,
        help="Max frame-index gap to treat as ‘small’ (no padding). Gaps above this will be filled by duplicating the previous frame."
    )
    return p.parse_args()

def main():
    args = parse_args()
    inp  = args.input_dir
    outp = args.output_dir
    thresh = args.threshold

    # 1) Gather and sort input files
    all_files = [f for f in os.listdir(inp)
                 if os.path.isfile(os.path.join(inp, f))]
    # Filter only images with numeric names and a common extension
    valid = []
    for fn in all_files:
        name, ext = os.path.splitext(fn)
        if name.isdigit() and ext.lower() in (".png", ".jpg", ".jpeg", ".bmp"):
            valid.append(fn)
    if not valid:
        print("❌ No valid image files found in", inp, file=sys.stderr)
        sys.exit(1)

    # Sort by numeric index
    valid.sort(key=lambda f: int(os.path.splitext(f)[0]))

    # Determine zero-padding width and extension
    sample_stem, sample_ext = os.path.splitext(valid[0])
    pad_width = len(sample_stem)
    ext = sample_ext

    # Prepare output directory
    os.makedirs(outp, exist_ok=True)

    out_idx     = 1
    prev_idx    = None
    prev_file   = None

    # 2) Loop and copy (with padding on large gaps)
    for fn in valid:
        curr_idx = int(os.path.splitext(fn)[0])

        if prev_idx is None:
            # First frame: just copy
            new_name = f"{out_idx:0{pad_width}d}{ext}"
            shutil.copyfile(
                os.path.join(inp, fn),
                os.path.join(outp, new_name)
            )
            prev_idx  = curr_idx
            prev_file = fn
            out_idx  += 1
            continue

        gap = curr_idx - prev_idx

        if gap > thresh:
            # Fill gap by duplicating prev_file (gap - 1) times
            missing = gap - 1
            for _ in range(missing):
                new_name = f"{out_idx:0{pad_width}d}{ext}"
                shutil.copyfile(
                    os.path.join(inp, prev_file),
                    os.path.join(outp, new_name)
                )
                out_idx += 1

        # Copy the current frame
        new_name = f"{out_idx:0{pad_width}d}{ext}"
        shutil.copyfile(
            os.path.join(inp, fn),
            os.path.join(outp, new_name)
        )
        prev_idx  = curr_idx
        prev_file = fn
        out_idx  += 1

    total = out_idx - 1
    print(f"✅ Prepared sequence with {total} frames in {outp}")

if __name__ == "__main__":
    main()
