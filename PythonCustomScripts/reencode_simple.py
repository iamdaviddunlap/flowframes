#!/usr/bin/env python3
import argparse
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Re-encode a video to H.265 @ a given framerate with optimized defaults"
    )
    # quality level
    parser.add_argument(
        "-c", "--crf", type=int, default=19,
        help="quality level (CRF). Lower = higher quality (default: 19)"
    )
    # target framerate
    parser.add_argument(
        "-r", "--framerate", type=int, required=True,
        help="target output framerate (e.g. 60)"
    )
    parser.add_argument("input", help="path to source video")
    parser.add_argument("output", help="path for encoded output")
    args = parser.parse_args()

    # hard-coded speed/quality knobs
    preset = "slow"  # one of: "veryslow", "slower", "slow", "medium"
    threads = 0  # 0 = auto detect all cores

    cmd = [
        "ffmpeg",
        "-i", args.input,
        "-vf", f"fps={args.framerate},format=yuv420p10le",
        "-c:v", "libx265",
        "-preset", preset,
        "-crf", str(args.crf),
        "-x265-params",
        "aq-mode=3:me=4:subme=7:ref=6:"
        "bframes=8:rc-lookahead=60:psy-rd=1.0:psy-rdoq=1.0",
        "-threads", str(threads),
        "-color_primaries", "bt709",
        "-color_trc", "bt709",
        "-colorspace", "bt709",
        "-color_range", "tv",
        "-tag:v", "hvc1",
        "-c:a", "copy",
        "-movflags", "+faststart",
        args.output
    ]

    print("Running:\n  " + " \\\n  ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()
