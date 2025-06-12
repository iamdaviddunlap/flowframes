import os
import shutil
import argparse
import re
import glob
from pathlib import Path

def generate_output_filename(counter, num_digits=8):
    """Generates a sequentially numbered filename for the output frames."""
    return f"output_frame_{counter:0{num_digits}d}.png"

def parse_frame_number(filename_str, pattern=r"^(\d+).*"):
    """
    Extracts the leading number from a filename.
    Example: "00000001.png" -> 1, "006_variant.jpg" -> 6
    Returns None if no number is found at the beginning.
    """
    match = re.match(pattern, os.path.basename(filename_str))
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None

def retime_image_sequence(input_dir, output_dir, target_frame_pace, image_extensions=None):
    """
    Generates a new image sequence with adjusted timings for Flowframes.

    Args:
        input_dir (str): Path to the directory containing unique input frames.
        output_dir (str): Path to the directory where the new sequence will be saved.
        target_frame_pace (int): The desired number of original video frame durations
                                 that one step in the Flowframes input should cover.
        image_extensions (list, optional): List of image extensions to consider (e.g., ['.png', '.jpg']).
                                           Defaults to ['.png', '.jpg', '.jpeg', '.bmp', '.tiff'].
    """
    if image_extensions is None:
        image_extensions = ['.png', '.jpg', '.jpeg', '.bmp', '.tiff', '.webp']

    input_path = Path(input_dir)
    output_path = Path(output_dir)

    if not input_path.is_dir():
        print(f"Error: Input directory '{input_dir}' not found.")
        return

    output_path.mkdir(parents=True, exist_ok=True)

    # 1. Discover and parse input frames
    parsed_frames = []
    print(f"Scanning input directory: {input_dir}")

    files_in_input_dir = []
    for ext in image_extensions:
        files_in_input_dir.extend(input_path.glob(f"*{ext}"))
        files_in_input_dir.extend(input_path.glob(f"*{ext.upper()}")) # Case-insensitive extensions

    if not files_in_input_dir:
        print(f"No image files found in '{input_dir}' with extensions: {image_extensions}")
        return
        
    for filepath in files_in_input_dir:
        frame_num = parse_frame_number(filepath.name)
        if frame_num is not None:
            parsed_frames.append({'original_num': frame_num, 'path': filepath})
        else:
            print(f"Warning: Could not parse frame number from '{filepath.name}'. Skipping.")

    if not parsed_frames:
        print("Error: No frames could be parsed from the input directory.")
        return

    # Sort frames by original frame number
    parsed_frames.sort(key=lambda x: x['original_num'])

    print(f"Found and parsed {len(parsed_frames)} unique frames.")

    # 2. Generate the new sequence
    output_filename_counter = 1
    num_output_digits = 8 # For filenames like output_frame_00000001.png

    # Handle the first frame
    if not parsed_frames:
        print("No frames to process.")
        return

    first_frame = parsed_frames[0]
    dest_path = output_path / generate_output_filename(output_filename_counter, num_output_digits)
    shutil.copy2(first_frame['path'], dest_path)
    print(f"Copied {first_frame['path'].name} -> {dest_path.name}")
    output_filename_counter += 1

    U_prev_path = first_frame['path']
    N_prev = first_frame['original_num']

    # Iterate through subsequent frames
    for i in range(1, len(parsed_frames)):
        current_frame = parsed_frames[i]
        N_curr = current_frame['original_num']
        U_curr_path = current_frame['path']

        original_gap = N_curr - N_prev

        if original_gap < 0:
            print(f"Warning: Frames appear out of order or duplicated original numbers detected. "
                  f"{U_curr_path.name} (frame {N_curr}) vs previous {U_prev_path.name} (frame {N_prev}). "
                  f"Skipping to avoid issues. Please ensure input frames are unique and correctly named.")
            # Option: could try to recover or just update N_prev and continue
            N_prev = N_curr
            U_prev_path = U_curr_path
            # Copy current frame anyway to not lose it, assuming it's the next actual unique frame
            dest_path = output_path / generate_output_filename(output_filename_counter, num_output_digits)
            shutil.copy2(U_curr_path, dest_path)
            print(f"Copied {U_curr_path.name} -> {dest_path.name} (after warning)")
            output_filename_counter += 1
            continue

        if original_gap == 0 and U_prev_path == U_curr_path: # Should not happen if input is unique
            print(f"Warning: Identical consecutive frames detected based on path and number: {U_curr_path.name}. Skipping.")
            continue
        elif original_gap == 0: # Different files with same frame number, problematic
             print(f"Warning: Different files have the same original frame number: {U_prev_path.name} and {U_curr_path.name} both original num {N_curr}. "
                   f"Copying the current one and continuing. Please check your input data.")
             # Treat as a minimal step
             dest_path = output_path / generate_output_filename(output_filename_counter, num_output_digits)
             shutil.copy2(U_curr_path, dest_path)
             print(f"Copied {U_curr_path.name} -> {dest_path.name} (after warning)")
             output_filename_counter += 1
             N_prev = N_curr
             U_prev_path = U_curr_path
             continue


        num_output_steps = max(1, round(original_gap / float(target_frame_pace)))

        # Insert hold frames (copies of U_prev)
        if num_output_steps > 1:
            num_hold_frames = num_output_steps - 1
            print(f"  Gap {N_prev}->{N_curr} (orig gap: {original_gap}, pace: {target_frame_pace}) -> {num_output_steps} steps. Inserting {num_hold_frames} copies of {U_prev_path.name}")
            for _ in range(num_hold_frames):
                dest_path = output_path / generate_output_filename(output_filename_counter, num_output_digits)
                shutil.copy2(U_prev_path, dest_path)
                # print(f"  Hold: Copied {U_prev_path.name} -> {dest_path.name}") # Verbose
                output_filename_counter += 1
        else:
             print(f"  Gap {N_prev}->{N_curr} (orig gap: {original_gap}, pace: {target_frame_pace}) -> {num_output_steps} step. Direct transition.")


        # Append current unique frame
        dest_path = output_path / generate_output_filename(output_filename_counter, num_output_digits)
        shutil.copy2(U_curr_path, dest_path)
        print(f"Copied {U_curr_path.name} -> {dest_path.name}")
        output_filename_counter += 1

        N_prev = N_curr
        U_prev_path = U_curr_path

    print("\nProcessing complete.")
    print(f"New image sequence saved to: {output_path.resolve()}")
    print(f"Total output frames: {output_filename_counter - 1}")

    print("\nIMPORTANT INSTRUCTIONS FOR FLOWFRAMES:")
    print("1. When importing this new image sequence into Flowframes:")
    print("   >>> YOU MUST DISABLE FRAME DE-DUPLICATION <<<")
    print("   This script intentionally creates duplicate frames to control timing.")
    print("   If Flowframes' de-duplication is ON, it will remove these intended duplicates,")
    print("   and the timing will be incorrect.")
    print("2. Flowframes will attempt to interpolate between all frames, including the")
    print("   identical 'hold' frames (e.g., 'frame A' -> 'frame A'). Ideally, this results")
    print("   in a still frame. However, be aware that some interpolation AI might produce")
    print("   minor artifacts or 'choppy output' in such scenarios, especially in")
    print("   dark/low-contrast scenes. This is a characteristic of the interpolation AI.")
    print("3. Adjust the 'TARGET_FRAME_PACE' in this script and re-run if you want to")
    print("   change how aggressively the timing is smoothed or how closely it follows")
    print("   the original frame gaps.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Re-times an image sequence with inconsistent unique frames for Flowframes.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("input_dir", type=str, help="Directory containing the unique input image frames.")
    parser.add_argument("output_dir", type=str, help="Directory where the new re-timed image sequence will be saved.")
    parser.add_argument(
        "--pace",
        type=int,
        default=5,
        dest="target_frame_pace",
        help="TARGET_FRAME_PACE: The ideal number of original video frame durations \n"
             "that one step in the Flowframes input sequence should represent. \n"
             "Smaller values try to match original timing more closely. \n"
             "Larger values smooth timing more. (Default: 5)"
    )
    parser.add_argument(
        "--ext",
        type=str,
        nargs='+',
        default=['.png', '.jpg', '.jpeg', '.bmp', '.tiff', '.webp'],
        dest="image_extensions",
        help="List of image extensions to process (e.g., .png .jpg). (Default: .png .jpg .jpeg .bmp .tiff .webp)"
    )

    args = parser.parse_args()

    # Ensure extensions start with a dot if user forgets
    args.image_extensions = [f".{ext.lstrip('.')}" for ext in args.image_extensions]


    if args.target_frame_pace <= 0:
        print("Error: TARGET_FRAME_PACE must be a positive integer.")
    else:
        retime_image_sequence(args.input_dir, args.output_dir, args.target_frame_pace, args.image_extensions)