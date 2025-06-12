#!/usr/bin/env python3
"""
Re-encode a video to H.265 or H.264 at a precise, constant framerate
with robust stop/resume capabilities.

This script segments a video for encoding, allowing it to be stopped and
resumed from the last completed segment. It forces a constant framerate (CFR)
output, which is critical for ensuring the final video's framerate exactly
matches the target, especially when the source is Variable Frame Rate (VFR).
"""

import argparse
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

# --- Globals ---
stop_requested_event = threading.Event()
force_stop_requested_event = threading.Event()  # For immediate shutdown on second Ctrl+C

# --- Default FFmpeg Settings ---
# H.265 (libx265) Defaults
DEFAULT_H265_CRF = 19
DEFAULT_H265_PIXEL_FORMAT = "yuv420p10le"  # 10-bit for higher quality H.265
DEFAULT_X265_PARAMS = "aq-mode=3:me=4:subme=7:ref=6:bframes=8:rc-lookahead=60:psy-rd=1.0:psy-rdoq=1.0"

# H.264 (libx264) Defaults
DEFAULT_H264_CRF = 18
DEFAULT_H264_PIXEL_FORMAT = "yuv420p"  # 8-bit for maximum compatibility with H.264
DEFAULT_X264_PARAMS = "aq-mode=1:aq-strength=1.0:me=umh:subme=10:psy_rd=1.0:trellis=2:ref=8:bframes=8:rc-lookahead=60"

# Common Defaults
DEFAULT_PRESET = "slow"
DEFAULT_AUDIO_BITRATE = "192k"
DEFAULT_SEGMENT_LENGTH = 5  # seconds
DEFAULT_THREADS = 0  # 0 = let the encoder pick automatically

# Technical Timeouts and Levels
FFPROBE_TIMEOUT = 30
FFMPEG_LOG_LEVEL = "warning"
FFMPEG_SHUTDOWN_TIMEOUT_Q = 10
FFMPEG_SHUTDOWN_TIMEOUT_SIG = 8
FFMPEG_SHUTDOWN_TIMEOUT_TERM = 5


def signal_handler(signum, frame):
    """Handle Ctrl+C signals for graceful and forceful shutdown."""
    global stop_requested_event, force_stop_requested_event
    if not stop_requested_event.is_set():
        print("\nCtrl+C detected. Requesting graceful stop...", file=sys.stderr)
        stop_requested_event.set()
    elif not force_stop_requested_event.is_set():
        print("\nCtrl+C detected again. Requesting immediate shutdown...", file=sys.stderr)
        force_stop_requested_event.set()
    else:
        print("\nMultiple Ctrl+C signals received. Shutdown already in progress.", file=sys.stderr)


def parse_arguments():
    """Parse command-line arguments and perform initial validation."""
    parser = argparse.ArgumentParser(
        description="Re-encode video to H.265/H.264 with segments and resume.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("input", help="Path to the source video file.")
    parser.add_argument("output", help="Path for the final encoded output file.")

    # Codec selection
    parser.add_argument("--codec", type=str, default='h265', choices=['h265', 'h264'],
                        help="Video codec to use for encoding (default: h265).")
    
    # Encoding parameters
    parser.add_argument("-c", "--crf", type=int, default=None,
                        help=f"CRF for the chosen codec (h265 default: {DEFAULT_H265_CRF}, h264 default: {DEFAULT_H264_CRF}).")
    parser.add_argument("-p", "--preset", type=str, default=DEFAULT_PRESET,
                        choices=['ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium',
                                 'slow', 'slower', 'veryslow', 'placebo'],
                        help=f"x265/x264 encoding preset (default: {DEFAULT_PRESET}).")
    parser.add_argument("-r", "--framerate", type=int, required=True,
                        help="Target output framerate (required).")
    parser.add_argument("--pix_fmt", type=str, default=None,
                        help=f"Output pixel format (h265 default: {DEFAULT_H265_PIXEL_FORMAT}, h264 default: {DEFAULT_H264_PIXEL_FORMAT}).")
    parser.add_argument("--x265-params", type=str, default=DEFAULT_X265_PARAMS,
                        help="Custom libx265 parameters (used only with --codec h265).")
    parser.add_argument("--x264-params", type=str, default=DEFAULT_X264_PARAMS,
                        help="Custom libx264 parameters (used only with --codec h264).")
    parser.add_argument("--threads", type=int, default=DEFAULT_THREADS,
                        help=f"Number of CPU threads for the encoder (default: {DEFAULT_THREADS} = auto).")
    parser.add_argument("--audio-bitrate", type=str, default=DEFAULT_AUDIO_BITRATE,
                        help=f"Audio bitrate for re-encoding (e.g., '192k'). Required for A/V sync (default: {DEFAULT_AUDIO_BITRATE}).")

    # Segmentation and file handling
    parser.add_argument("--segment-length", type=int, default=DEFAULT_SEGMENT_LENGTH,
                        help=f"Segment length in seconds (default: {DEFAULT_SEGMENT_LENGTH}).")
    parser.add_argument("--segments-dir", type=str, default=None,
                        help="Custom directory for segment files.")
    parser.add_argument("--keep-segments", action="store_true",
                        help="Keep segment files after concatenation.")
    parser.add_argument("--overwrite-final-output", action="store_true",
                        help="Overwrite final output file if it exists.")

    args = parser.parse_args()

    # Set codec-specific defaults if user did not provide them
    if args.codec == 'h264':
        if args.crf is None:
            args.crf = DEFAULT_H264_CRF
        if args.pix_fmt is None:
            args.pix_fmt = DEFAULT_H264_PIXEL_FORMAT
    else:  # h265
        if args.crf is None:
            args.crf = DEFAULT_H265_CRF
        if args.pix_fmt is None:
            args.pix_fmt = DEFAULT_H265_PIXEL_FORMAT

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()

    if not input_path.is_file():
        parser.error(f"Input file not found: {input_path}")
    if os.path.normcase(str(input_path)) == os.path.normcase(str(output_path)):
        parser.error("Input and output file paths cannot be the same.")
    if args.segment_length <= 0:
        parser.error("--segment-length must be > 0")
    if args.framerate <= 0:
        parser.error("--framerate must be > 0")
    if args.threads < 0:
        parser.error("--threads must be >= 0 (0 = auto)")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    return args


def get_stream_info(file_path: Path, stream_type: str = "v", timeout: int = FFPROBE_TIMEOUT) -> dict | None:
    """Gets stream information using ffprobe."""
    cmd = [
        "ffprobe", "-loglevel", FFMPEG_LOG_LEVEL,
        "-select_streams", f"{stream_type}:0",
        "-show_streams", "-show_format",
        "-of", "json", str(file_path)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=timeout)
        data = json.loads(result.stdout)
        return data
    except FileNotFoundError:
        print("Error: ffprobe not found. Ensure it's in your PATH.", file=sys.stderr)
    except subprocess.CalledProcessError:
        pass  # A non-zero exit code might just mean no stream of that type exists.
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        print(f"Error parsing ffprobe output for {file_path} (stream {stream_type}): {e}", file=sys.stderr)
    except subprocess.TimeoutExpired:
        print(f"Error: ffprobe timed out for {file_path} (stream {stream_type})", file=sys.stderr)
    return None


def get_file_duration(file_path: Path, timeout: int = FFPROBE_TIMEOUT) -> float:
    """Get file duration, checking video stream first, then audio."""
    data = get_stream_info(file_path, "v", timeout)
    if data and "format" in data and "duration" in data["format"]:
        try:
            return float(data["format"]["duration"])
        except (ValueError, TypeError):
            pass

    data_audio = get_stream_info(file_path, "a", timeout)
    if data_audio and "format" in data_audio and "duration" in data_audio["format"]:
        try:
            return float(data_audio["format"]["duration"])
        except (ValueError, TypeError):
            pass
    return -1.0


def has_audio_stream(file_path: Path, timeout: int = FFPROBE_TIMEOUT) -> bool:
    """Check if the file contains an audio stream."""
    data = get_stream_info(file_path, "a", timeout)
    return bool(data and "streams" in data and len(data["streams"]) > 0)


def calculate_segments(total_duration: float, segment_length: float) -> list:
    """Calculate start and end times for each video segment."""
    if total_duration <= 0 or segment_length <= 0:
        return []
    
    segments = []
    current_time = 0.0
    while current_time < total_duration:
        start_time = current_time
        end_time = min(current_time + segment_length, total_duration)
        segment_duration = end_time - start_time
        
        # Avoid creating tiny, useless segments at the very end
        if segment_duration < 0.01:
            break
            
        segments.append({"start": start_time, "end": end_time, "duration": segment_duration})
        current_time += segment_length
        
    return segments


def _monitor_ffmpeg_output(pipe, log_prefix):
    """Read ffmpeg's output pipe and print relevant lines."""
    if pipe:
        for line in iter(pipe.readline, ''):
            line_strip = line.strip()
            # Only print lines containing keywords to keep logs clean
            if line_strip and ("error" in line_strip.lower() or "warning" in line_strip.lower()):
                print(f"[{log_prefix}] {line_strip}", flush=True)
        pipe.close()


def run_ffmpeg_command(cmd_list: list, log_prefix: str = "ffmpeg") -> bool:
    """Execute an FFmpeg command with robust process management and shutdown."""
    global stop_requested_event, force_stop_requested_event
    print(f"[{log_prefix}] Running: {shlex.join(cmd_list)}", flush=True)

    creationflags = 0
    if sys.platform == "win32":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP

    process = None
    try:
        process = subprocess.Popen(
            cmd_list,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors='replace',
            creationflags=creationflags
        )

        output_monitor_thread = None
        if process.stdout:
            output_monitor_thread = threading.Thread(
                target=_monitor_ffmpeg_output, args=(process.stdout, log_prefix)
            )
            output_monitor_thread.daemon = True
            output_monitor_thread.start()

        while process.poll() is None:
            if force_stop_requested_event.is_set():
                print(f"[{log_prefix}] Force stop. Killing ffmpeg immediately.", file=sys.stderr)
                process.kill()
                break

            if stop_requested_event.is_set():
                print(f"[{log_prefix}] Graceful stop requested.", file=sys.stderr)
                # Try sending 'q' first for a clean shutdown
                try:
                    if process.stdin and not process.stdin.closed:
                        process.stdin.write('q\n')
                        process.stdin.flush()
                        process.stdin.close()
                    process.wait(timeout=FFMPEG_SHUTDOWN_TIMEOUT_Q)
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    # If 'q' fails or times out, escalate
                    if process.poll() is None:
                        if sys.platform == "win32":
                            os.kill(process.pid, signal.CTRL_BREAK_EVENT)
                        else:
                            process.send_signal(signal.SIGINT)
                        try:
                            process.wait(timeout=FFMPEG_SHUTDOWN_TIMEOUT_SIG)
                        except subprocess.TimeoutExpired:
                             if process.poll() is None:
                                process.terminate()
                                try:
                                    process.wait(timeout=FFMPEG_SHUTDOWN_TIMEOUT_TERM)
                                except subprocess.TimeoutExpired:
                                    if process.poll() is None:
                                        process.kill()
                break # Exit loop after handling stop request

            time.sleep(0.5)

        if output_monitor_thread:
            output_monitor_thread.join(timeout=2)
            
        return process.returncode == 0

    except FileNotFoundError:
        print("Error: ffmpeg not found. Ensure it's in your PATH.", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error running/managing ffmpeg command: {e}", file=sys.stderr)
        if process and process.poll() is None:
            process.kill()
        return False


def encode_segment(
        input_file: Path, output_part_file: Path, segment_info: dict,
        config: argparse.Namespace, audio_present: bool
) -> bool:
    """Constructs and runs the FFmpeg command for a single video segment."""
    start_time_str = f"{segment_info['start']:.6f}"
    duration_str = f"{segment_info['duration']:.6f}"

    # Use -vsync cfr to normalize input timestamps, critical for VFR sources
    base_cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", FFMPEG_LOG_LEVEL, "-y",
        "-vsync", "cfr",
        "-i", str(input_file),
        "-ss", start_time_str,
        "-t", duration_str
    ]

    # Use both -vf fps and output -r to ensure a precise constant framerate
    video_cmd = [
        "-vf", f"fps={config.framerate}:round=near,format={config.pix_fmt}",
        "-r", str(config.framerate),
        "-preset", config.preset,
        "-crf", str(config.crf),
        "-color_primaries", "bt709", "-color_trc", "bt709",
        "-colorspace", "bt709", "-color_range", "tv",
        "-movflags", "+faststart", "-reset_timestamps", "1"
    ]

    # Codec-specific command construction
    if config.codec == 'h265':
        final_x265_params = config.x265_params
        if config.threads > 0:
            if 'pools=' in config.x265_params.lower():
                print("Error: Cannot specify --threads and 'pools=' in --x265-params.", file=sys.stderr)
                return False
            final_x265_params = f"pools={config.threads}:{final_x265_params}"
        
        codec_specific_cmd = [
            "-c:v", "libx265",
            "-x265-params", final_x265_params,
            "-tag:v", "hvc1"
        ]
    else:  # h264
        codec_specific_cmd = [
            "-c:v", "libx264",
            "-x264-params", config.x264_params
        ]
        if config.threads > 0:
            codec_specific_cmd.extend(["-threads", str(config.threads)])
    
    # Assemble final command
    cmd = base_cmd + video_cmd + codec_specific_cmd

    if audio_present:
        # Re-encode audio to ensure A/V sync with the new constant framerate
        cmd.extend(["-c:a", "aac", "-b:a", config.audio_bitrate])
    else:
        cmd.extend(["-an"])
    
    cmd.extend(["-f", "mp4", str(output_part_file)])

    return run_ffmpeg_command(cmd, log_prefix=f"Segment {output_part_file.name.replace('.part', '')}")


def concatenate_segments(segment_files: list, final_output_file: Path) -> bool:
    """Concatenate encoded segment files into the final output video."""
    if not segment_files:
        print("No segment files to concatenate.", file=sys.stderr)
        return False

    filelist_path = final_output_file.parent / f"{final_output_file.stem}_filelist_temp.txt"
    try:
        with open(filelist_path, "w", encoding="utf-8") as f:
            for segment_path_obj in segment_files:
                # Use shlex.quote for robust path handling on POSIX
                if sys.platform != "win32":
                    path_str = shlex.quote(str(segment_path_obj.resolve()))
                    f.write(f"file {path_str}\n")
                else: # Windows quoting is different for ffprobe's concat demuxer
                    path_str = str(segment_path_obj.resolve()).replace('\\', '/')
                    f.write(f"file '{path_str}'\n")

        print(f"Concatenating {len(segment_files)} segments into {final_output_file}")
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", FFMPEG_LOG_LEVEL, "-y",
            "-f", "concat", "-safe", "0",
            "-i", str(filelist_path),
            "-c", "copy", "-movflags", "+faststart",
            "-fflags", "+genpts",
            str(final_output_file)
        ]
        return run_ffmpeg_command(cmd, log_prefix="Concatenate")
    finally:
        if filelist_path.exists():
            try:
                filelist_path.unlink()
            except OSError as e:
                print(f"Warning: Could not delete temp filelist {filelist_path}: {e}", file=sys.stderr)


def save_rerun_command(segments_dir: Path):
    """Saves the exact command used to a file for easy resuming."""
    rerun_file_path = segments_dir / "rerun_command.txt"
    try:
        if sys.platform == "win32":
            # list2cmdline is robust for cmd.exe and PowerShell
            command_to_save = subprocess.list2cmdline([sys.executable] + sys.argv)
        else:
            # shlex.quote is robust for POSIX shells
            py_exe = shlex.quote(sys.executable)
            args = " ".join([shlex.quote(arg) for arg in sys.argv])
            command_to_save = f"{py_exe} {args}"

        rerun_file_path.write_text(command_to_save, encoding='utf-8')
        print(f"ℹ️ To resume, copy the command from: {rerun_file_path}")
    except Exception as e:
        print(f"Warning: Could not save the rerun command file: {e}", file=sys.stderr)


def print_config_summary(config, input_f, output_f, segments_d):
    """Prints a summary of the encoding configuration."""
    print("--- Configuration Summary ---")
    print(f"  Input:            {input_f}")
    print(f"  Output:           {output_f}")
    print(f"  Segments Dir:     {segments_d}")
    print(f"  Codec:            {config.codec}")
    print(f"  CRF: {config.crf}, Preset: {config.preset}, FPS: {config.framerate}, Threads: {config.threads}")
    if config.codec == 'h265':
        print(f"  x265-params:      {config.x265_params}")
    else:
        print(f"  x264-params:      {config.x264_params}")
    print(f"  Audio Bitrate:    {config.audio_bitrate} (re-encoded)")
    print(f"  Segment Length:   {config.segment_length}s")
    print("-----------------------------")


def main_process():
    """Main script logic orchestrating the encoding process."""
    global stop_requested_event, force_stop_requested_event
    config = parse_arguments()
    signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGBREAK'): # For Windows
        signal.signal(signal.SIGBREAK, signal_handler)

    input_file = Path(config.input).resolve()
    final_output_file = Path(config.output).resolve()
    output_file_ext = final_output_file.suffix

    if config.segments_dir:
        segments_dir = Path(config.segments_dir).resolve()
    else:
        segments_dir = final_output_file.parent / f"{input_file.stem}_segments"

    try:
        segments_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Error: Could not create segments dir {segments_dir}: {e}", file=sys.stderr)
        sys.exit(1)

    print_config_summary(config, input_file, final_output_file, segments_dir)
    save_rerun_command(segments_dir)

    input_file_has_audio = has_audio_stream(input_file)
    total_duration_input = get_file_duration(input_file)
    if total_duration_input <= 0:
        print(f"Error: Could not determine valid duration for input {input_file}.", file=sys.stderr)
        sys.exit(1)
    print(f"Total input video duration: {total_duration_input:.2f}s")

    if final_output_file.exists():
        if config.overwrite_final_output:
            print(f"--overwrite-final-output specified. Deleting existing: {final_output_file}")
            try:
                final_output_file.unlink()
            except OSError as e:
                print(f"Error: Could not delete {final_output_file}: {e}", file=sys.stderr)
                sys.exit(1)
        else:
            print(f"Final output file {final_output_file} already exists. Use --overwrite-final-output.",
                  file=sys.stderr)
            sys.exit(0)

    # Clean up any orphaned .part files from a previously failed run
    for part_item in segments_dir.glob(f"*{output_file_ext}.part"):
        if part_item.is_file():
            print(f"Deleting orphaned part file from previous run: {part_item.name}")
            part_item.unlink(missing_ok=True)

    calculated_segments_info = calculate_segments(total_duration_input, config.segment_length)
    if not calculated_segments_info:
        print("Error: No segments calculated. Check input duration and segment length.", file=sys.stderr)
        sys.exit(1)
    print(f"Calculated {len(calculated_segments_info)} segments.")

    # --- Resume Logic: Verify existing segments ---
    completed_segment_paths = []
    resume_segment_idx = 0
    for i, seg_info in enumerate(calculated_segments_info):
        segment_number = i + 1
        segment_base_name = f"{input_file.stem}_seg{segment_number:04d}"
        segment_final_file = segments_dir / f"{segment_base_name}{output_file_ext}"

        if segment_final_file.exists():
            actual_seg_duration = get_file_duration(segment_final_file)
            expected_seg_duration = seg_info['duration']
            # Tolerate small duration differences
            duration_tolerance = max(0.1, expected_seg_duration * 0.02)
            if actual_seg_duration > 0 and abs(actual_seg_duration - expected_seg_duration) < duration_tolerance:
                completed_segment_paths.append(segment_final_file)
                resume_segment_idx = i + 1
            else:
                print(
                    f"Segment {segment_number} ({segment_final_file.name}) invalid (expected ~{expected_seg_duration:.2f}s, got {actual_seg_duration:.2f}s). Deleting.",
                    file=sys.stderr)
                segment_final_file.unlink(missing_ok=True)
                break # Re-encode from this point
        else:
            break # First missing segment found, stop scanning

    if resume_segment_idx > 0:
        print(f"Resuming from segment {resume_segment_idx + 1}.")

    # --- Encoding Loop ---
    for i in range(resume_segment_idx, len(calculated_segments_info)):
        if stop_requested_event.is_set():
            print("Stop requested. Exiting segment encoding loop.", file=sys.stderr)
            break

        segment_number = i + 1
        segment_info = calculated_segments_info[i]
        segment_base_name = f"{input_file.stem}_seg{segment_number:04d}"
        segment_final_file = segments_dir / f"{segment_base_name}{output_file_ext}"
        segment_part_file = segments_dir / f"{segment_base_name}{output_file_ext}.part"

        print(f"\n--- Encoding Segment {segment_number}/{len(calculated_segments_info)} ---")
        print(f"Time: {segment_info['start']:.2f}s to {segment_info['end']:.2f}s (Duration: {segment_info['duration']:.2f}s)")

        segment_part_file.unlink(missing_ok=True) # Clean up before starting
        success = encode_segment(input_file, segment_part_file, segment_info, config, input_file_has_audio)

        if not success:
            print(f"Segment {segment_number} failed or was stopped.", file=sys.stderr)
            break # Exit loop on failure or stop
        
        try:
            shutil.move(segment_part_file, segment_final_file)
            print(f"✅ Successfully encoded segment: {segment_final_file.name}")
            completed_segment_paths.append(segment_final_file)
        except Exception as e:
            print(f"Error: Failed to move {segment_part_file} to {segment_final_file}: {e}", file=sys.stderr)
            break

    # --- Final Outcome ---
    if stop_requested_event.is_set():
        print(f"\nEncoding stopped by user. {len(completed_segment_paths)} segments reliably completed.", file=sys.stderr)
        print("To resume, run the same command again.")
        sys.exit(0)
    
    if len(completed_segment_paths) == len(calculated_segments_info):
        print("\nAll segments encoded successfully. Starting concatenation...")
        concat_success = concatenate_segments(completed_segment_paths, final_output_file)
        if concat_success:
            print(f"🎉 Successfully concatenated segments into {final_output_file}")
            if not config.keep_segments:
                print(f"Cleaning up segments directory: {segments_dir}")
                try:
                    shutil.rmtree(segments_dir)
                except OSError as e:
                    print(f"Warning: Failed to delete segments dir {segments_dir}: {e}", file=sys.stderr)
            print("Encoding complete.")
        else:
            print(f"Error: Failed to concatenate. Segments kept in {segments_dir}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"\nEncoding failed or was incomplete. {len(completed_segment_paths)} segments completed.", file=sys.stderr)
        print(f"Check logs for errors. Segments directory: {segments_dir}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main_process()
    except SystemExit:
        raise
    except Exception as e:
        print(f"An unexpected fatal error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)
