import json
import subprocess
import argparse
import logging
import sys
import os
import multiprocessing
import glob  # Import the glob module
from tqdm import tqdm

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def download_worker(task_args):
    """
    The worker function for a single download task.
    This function is executed by each process in the multiprocessing pool.

    Args:
        task_args (tuple): A tuple containing (item, output_dir).
                           - item (dict): The video dictionary from the JSON.
                           - output_dir (str): The directory to save the video.

    Returns:
        tuple: A tuple containing (bool, str) for success/failure and the video_info_string.
               e.g., (True, 'video_id_...') or (False, 'video_id_...')
    """
    item, output_dir = task_args
    video_info_string = None  # Initialize in case of early failure

    try:
        # Get the full video info string (e.g., 'bjtnAh_wz1c_000002_000012')
        video_info_string = item['videoID']

        # This logic is now robust against underscores in the video ID
        parts = video_info_string.split('_')
        if len(parts) < 3:
            raise ValueError("videoID string does not have enough parts.")

        end_time = parts[-1]        # The last part is the end time
        start_time = parts[-2]      # The second-to-last part is the start time
        video_id = "_".join(parts[:-2]) # Join everything else to form the video_id

        start_seconds = int(start_time)
        end_seconds = int(end_time)

    except (KeyError, ValueError) as e:
        logging.warning(f"Skipping entry due to malformed data: {item}. Error: {e}")
        # Return failure with a placeholder if video_info_string could not be parsed
        return False, (video_info_string or f"malformed_data:_{item}")

    # Define the full output path for the video file
    output_path_template = os.path.join(output_dir, f"{video_info_string}.%(ext)s")
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"

    command = [
        'yt-dlp',
        '--quiet', '--no-warnings',
        '-o', output_path_template,  # Use -o as a shorthand for --output
        '--download-sections', f"*{start_seconds}-{end_seconds}",
        '--force-keyframes-at-cuts',
        '--remux-video', 'mp4',
        youtube_url
    ]

    try:
        # Using capture_output=True and text=True to get stdout/stderr if needed
        subprocess.run(command, check=True, capture_output=True, text=True)
        return True, video_info_string
    except FileNotFoundError:
        logging.error("CRITICAL: 'yt-dlp' command not found. Please ensure it's installed and in your PATH.")
        # This error is critical and will likely affect all workers, but we return failure for this task.
        return False, video_info_string
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to download {video_info_string}. Reason: {e.stderr.strip()}")
        return False, video_info_string
    except Exception as e:
        logging.error(f"An unexpected error occurred for {video_info_string}: {e}")
        return False, video_info_string

def process_downloads(json_file_path, output_dir, num_jobs):
    """
    Orchestrates the parallel downloading of video segments, skipping existing files.

    Args:
        json_file_path (str): The path to the input JSON file.
        output_dir (str): The directory to save downloaded videos.
        num_jobs (int): The number of parallel processes to use.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            video_data = json.load(f)
        logging.info(f"Loaded {len(video_data)} video entries from '{json_file_path}'.")
    except FileNotFoundError:
        logging.error(f"Error: The file '{json_file_path}' was not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        logging.error(f"Error: Failed to decode JSON from '{json_file_path}'. Check format.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Output directory set to '{output_dir}'.")

    # --- NEW: Pre-filter tasks to implement resumability ---
    tasks_to_run = []
    skipped_count = 0
    logging.info("Checking for existing files to skip...")

    for item in tqdm(video_data, desc="Scanning for existing files"):
        try:
            video_info_string = item['videoID']
            # Create a pattern to match filename with any extension
            file_pattern = os.path.join(output_dir, f"{video_info_string}.*")
            # Use glob.glob to find if any file matches the pattern
            if glob.glob(file_pattern):
                skipped_count += 1
            else:
                tasks_to_run.append((item, output_dir))
        except KeyError:
            # This will be handled properly by the worker, just pass it through
            tasks_to_run.append((item, output_dir))

    if not tasks_to_run:
        logging.info("All video files already exist. Nothing to download.")
        logging.info(f"Total files skipped: {skipped_count}")
        return

    logging.info(f"Found {skipped_count} existing files. Queuing {len(tasks_to_run)} new downloads.")
    logging.info(f"Starting downloads with {num_jobs} parallel jobs...")

    success_count = 0
    failure_count = 0

    with multiprocessing.Pool(processes=num_jobs) as pool:
        # Use imap_unordered for efficiency, as download order doesn't matter
        results_iterator = pool.imap_unordered(download_worker, tasks_to_run)

        for _ in tqdm(range(len(tasks_to_run)), desc="Downloading Videos"):
            success, video_id = next(results_iterator)
            if success:
                success_count += 1
            else:
                failure_count += 1

    logging.info("--- Download Summary ---")
    logging.info(f"Successfully downloaded: {success_count}")
    logging.info(f"Skipped (already exist): {skipped_count}")
    logging.info(f"Failed to download:    {failure_count}")
    logging.info("------------------------")

def main():
    """Main function to parse arguments and start the download process."""
    parser = argparse.ArgumentParser(
        description="Download video segments in parallel from a JSON file using yt-dlp. Skips existing files.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "json_file",
        type=str,
        help="Path to the JSON file containing video information."
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default="video_downloads",
        help="Directory to save downloaded videos. (Default: 'video_downloads')"
    )
    parser.add_argument(
        "-j", "--jobs",
        type=int,
        default=4,
        help="Number of parallel download jobs to run. (Default: 4)"
    )

    args = parser.parse_args()

    if args.jobs <= 0:
        logging.error("Number of jobs must be a positive integer.")
        sys.exit(1)

    process_downloads(args.json_file, args.output_dir, args.jobs)

if __name__ == "__main__":
    main()
