#!/usr/bin/env python3
import os
import re
import sys
import math
import argparse
import shutil
import subprocess

def resample_and_encode(folder, orig_fps, target_fps,
                        output, audio, gpu, preset, rate_ctrl,
                        cq, qp, pix_fmt):
    # 1) scan & sort
    patt = re.compile(r'^(.*?)(\d+)(\.png)$', re.IGNORECASE)
    entries = []
    for fn in os.listdir(folder):
        m = patt.match(fn)
        if not m: continue
        prefix, num, ext = m.groups()
        entries.append((fn, prefix, int(num), len(num), ext))
    if not entries:
        sys.exit(f"No matching .png in {folder!r}")
    entries.sort(key=lambda e: e[2])
    N = len(entries)

    # 2) sanity checks
    if target_fps == orig_fps:
        print(f"{orig_fps=} == {target_fps=}; skipping resample.")
        frame_indices = list(range(N))
    else:
        if target_fps > orig_fps:
            sys.exit("Up-sampling not supported.")
        # how many frames we want, rounded
        ideal = N * target_fps / orig_fps
        K     = max(int(round(ideal)), 1)
        # pick via floor spacing
        ratio = orig_fps / target_fps
        keep = sorted({int(math.floor(k * ratio)) for k in range(K)})
        keep = [i for i in keep if 0 <= i < N]
        # delete unneeded
        keep_set = set(keep)
        for idx,(fn, *_ ) in enumerate(entries):
            if idx not in keep_set:
                os.remove(os.path.join(folder, fn))
                print(f"Removed {fn}")
        # rename survivors
        kept = [entries[i] for i in keep]
        for new_i,(old, pref, _, pad, ext) in enumerate(kept, start=1):
            new_name = f"{pref}{str(new_i).zfill(pad)}{ext}"
            if old != new_name:
                os.rename(os.path.join(folder, old),
                          os.path.join(folder, new_name))
                print(f"Renamed {old} → {new_name}")
        # duplicate last if rounding underflowed
        current = len(keep)
        if current < K:
            last_pref, last_pad, last_ext = kept[-1][1], kept[-1][3], kept[-1][4]
            last_name = f"{last_pref}{str(current).zfill(last_pad)}{last_ext}"
            for i in range(1, K-current+1):
                new_idx  = current + i
                new_name = f"{last_pref}{str(new_idx).zfill(last_pad)}{last_ext}"
                shutil.copy(os.path.join(folder, last_name),
                            os.path.join(folder, new_name))
                print(f"Duplicated {last_name} → {new_name}")
        frame_indices = None  # not used below

    # 3) build FFmpeg command
    # derive input pattern from renamed files
    sample = sorted(os.listdir(folder))[0]
    m = patt.match(sample)
    if not m:
        sys.exit("Unexpected renaming; no PNG found.")
    prefix, _, ext = m.group(1), m.group(2), m.group(3)
    pad = len(m.group(2))
    input_pattern = os.path.join(folder, f"{prefix}%0{pad}d{ext}")

    cmd = [
        "ffmpeg",
        "-hwaccel", "cuda",
        "-framerate", str(target_fps),
        "-start_number", "1",
        "-i", input_pattern
    ]
    if audio:
        cmd += ["-i", audio, "-shortest", "-c:a", "copy"]
    cmd += [
        "-c:v", "hevc_nvenc",
        "-gpu", str(gpu),
        "-preset", preset,
        "-rc", rate_ctrl
    ]
    if rate_ctrl.lower() == "vbr":
        cmd += ["-cq", str(cq)]
    else:  # constqp
        cmd += ["-qp", str(qp)]
    cmd += ["-pix_fmt", pix_fmt, output]

    print("\nRunning FFmpeg:\n  " + " \\\n  ".join(cmd))
    subprocess.run(cmd, check=True)
    print(f"\n✅ Created {output}")

def main():
    p = argparse.ArgumentParser(
        description="Down-sample frames and encode video with NVENC HEVC."
    )
    p.add_argument("folder", help="PNG folder")
    p.add_argument("orig_fps",   type=float, help="Original FPS")
    p.add_argument("target_fps", type=float, help="Target FPS ≤ original")
    p.add_argument("-o","--output", default="out.mp4", help="Output video file")
    p.add_argument("-a","--audio", help="Optional audio track to mux")
    p.add_argument("--gpu",      type=int,   default=0,        help="NVENC GPU index")
    p.add_argument("--preset",   default="p7",
                   help="NVENC preset (p1…p7; p7=slowest/highest-quality)")
    p.add_argument("--rate-ctrl", default="vbr",
                   choices=["vbr","constqp"],
                   help="Rate control: vbr (CQ) or constqp (QP)")
    p.add_argument("--cq", type=int, default=19,
                   help="for vbr: constant quality (lower=better; 18–20 visually lossless)")
    p.add_argument("--qp", type=int, default=0,
                   help="for constqp: QP value (0=lossless; 1≈near-lossless)")
    p.add_argument("--pix_fmt", default="yuv420p",
                   help="pixel format (e.g. yuv420p or yuv444p if supported)")
    args = p.parse_args()

    if not os.path.isdir(args.folder):
        sys.exit(f"{args.folder!r} is not a directory")
    if args.target_fps > args.orig_fps:
        sys.exit("target_fps must be ≤ orig_fps")

    resample_and_encode(
        args.folder, args.orig_fps, args.target_fps,
        args.output, args.audio, args.gpu, args.preset,
        args.rate_ctrl, args.cq, args.qp, args.pix_fmt
    )

if __name__ == "__main__":
    main()
