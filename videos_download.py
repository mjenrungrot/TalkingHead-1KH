import argparse
import multiprocessing as mp
import os
from functools import partial
from time import time as timer

from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument("--input_list", type=str, required=True, help="List of youtube video ids")
parser.add_argument("--output_dir", type=str, default="data/youtube_videos", help="Location to download videos")
parser.add_argument("--num_workers", type=int, default=1, help="How many multiprocessing workers?")
args = parser.parse_args()


def download_video(output_dir, video_id):
    r"""Download video."""
    video_path = "%s/%s.mp4" % (output_dir, video_id)
    if not os.path.isfile(video_path):
        ytb_id = video_id
        down_video = " ".join(
            [
                "yt-dlp",
                "-f",
                "'bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio'",
                "--skip-unavailable-fragments",
                "--merge-output-format",
                "mp4",
                "https://www.youtube.com/watch?v=" + ytb_id,
                "--output",
                video_path,
                "--external-downloader",
                "aria2c",
                "--external-downloader-args",
                '"-x 16 -k 1M"',
            ]
        )
        print(down_video)
        status = os.system(down_video)
        if status != 0:
            print(f"video not found: {ytb_id}")
    else:
        print("File exists: %s" % (video_id))


if __name__ == "__main__":
    # Read list of videos.
    video_ids = []
    with open(args.input_list) as fin:
        for line in fin:
            video_ids.append(line.strip())

    # Create output folder.
    os.makedirs(args.output_dir, exist_ok=True)

    # Download videos.
    downloader = partial(download_video, args.output_dir)

    start = timer()
    pool_size = args.num_workers
    print("Using pool size of %d" % (pool_size))
    with mp.Pool(processes=pool_size) as p:
        _ = list(tqdm(p.imap_unordered(downloader, video_ids), total=len(video_ids)))
    print("Elapsed time: %.2f" % (timer() - start))


# python videos_download.py --input_list data_list/val_video_ids.txt --output_dir val/raw_videos
