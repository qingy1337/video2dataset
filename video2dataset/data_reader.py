"""classes and functions for downloading videos"""

import os
import uuid
import requests
import yt_dlp
from yt_dlp.utils import download_range_func
import io
import webvtt
import ffmpeg
import re
import itertools
from threading import Lock


def video2audio(video, audio_format, tmp_dir):
    """extract audio from video"""
    path = f"{tmp_dir}/{str(uuid.uuid4())}.{audio_format}"
    num_streams = len(ffmpeg.probe(video)["streams"])
    ffmpeg_args = {"f": audio_format}

    if int(num_streams) > 1:  # video has audio stream
        try:
            video = ffmpeg.input(video)
            (ffmpeg.output(video.audio, path, **ffmpeg_args).run(capture_stderr=True))
        except ffmpeg.Error as _:
            path = None
    else:
        path = None
    return path


def sub_to_dict(sub, dedupe=True, single=False) -> list:
    """Convert WebVTT to JSON, optionally removing duplicate lines"""

    captions = webvtt.read_buffer(io.StringIO(sub))
    dicts = [{"start": c.start, "end": c.end, "lines": c.lines} for c in captions]
    if dedupe:
        dicts = []
        prev_line = None
        for c in captions:
            if any("<c>" in l for l in c.lines):
                continue
            # Collect lines that are not dupes
            not_dupe_lines = []
            for line in c.lines:
                if not line.strip():
                    continue
                if line != prev_line:
                    not_dupe_lines.append(line)
                prev_line = line
            if not_dupe_lines:
                dicts.append({"start": c.start, "end": c.end, "lines": not_dupe_lines})
    if single:
        for d in dicts:
            d["line"] = "\n".join(d.pop("lines"))
    return dicts


def get_yt_meta(url, yt_metadata_args: dict) -> dict:
    """Return yt meta dict with meta data and/or subtitles
    yt_metadata_args is a dict of follwing format:
    yt_metadata_args = {
        'writesubtitles': 'first',
        'subtitleslangs': ['en'],
        'writeautomaticsub': True,
        'get_info': True
    }

    writesubtitles:    Whether to write subtitles for each provided language or just the first present
    writeautomaticsub: Write the automatically generated subtitles to a file
    subtitleslangs:    List of languages of the subtitles to download.
    get_info:          Whether to add info (title, description, tags etc) to the output.
    """

    write_subs = yt_metadata_args.get("writesubtitles", None)

    yt_metadata_args["skip_download"] = True
    yt_metadata_args["ignoreerrors"] = True
    yt_metadata_args["quiet"] = True

    info_dict, full_sub_dict = None, None

    with yt_dlp.YoutubeDL(yt_metadata_args) as yt:
        info_dict = yt.extract_info(url, download=False)
        if write_subs:
            full_sub_dict = {}
            for lang in yt_metadata_args["subtitleslangs"]:
                if lang not in info_dict["requested_subtitles"]:
                    continue
                sub_url = info_dict["requested_subtitles"][lang]["url"]
                res = requests.get(sub_url, timeout=10)
                sub = io.TextIOWrapper(io.BytesIO(res.content)).read()
                full_sub_dict[lang] = sub_to_dict(sub)

                if write_subs == "first":
                    break

        if yt_metadata_args["get_info"]:
            info_dict.pop("subtitles")
            info_dict.pop("requested_formats")
            info_dict.pop("formats")
            info_dict.pop("thumbnails")
            info_dict.pop("automatic_captions")
        else:
            info_dict = None

        yt_meta_dict = {"info": info_dict, "subtitles": full_sub_dict}

        return yt_meta_dict


def get_file_info(url):
    """returns info about the url (currently extension and modality)"""
    # TODO: make this nicer
    video_extensions = ["mp4", "webm", "mov", "avi", "mkv"]
    audio_extensions = ["mp3", "wav", "m4a"]
    for ext in video_extensions:
        if url.endswith(f".{ext}"):
            return ext, "video"
    for ext in audio_extensions:
        if url.endswith(f".{ext}"):
            return ext, "audio"
    return None


class WebFileDownloader:
    """Downloader class for mp4 links"""

    def __init__(self, timeout, tmp_dir, encode_formats):
        self.timeout = timeout
        self.tmp_dir = tmp_dir
        self.encode_formats = encode_formats

    def __call__(self, url):
        modality_paths = {}

        ext, modality = get_file_info(url)
        if not os.path.isfile(url):
            resp = requests.get(url, stream=True, timeout=self.timeout)
            byts = resp.content
        else:  # local files (don't want to delete)
            with open(url, "rb") as f:
                byts = f.read()

        modality_path = f"{self.tmp_dir}/{str(uuid.uuid4())}.{ext}"
        with open(modality_path, "wb") as f:
            f.write(byts)

        modality_paths[modality] = modality_path

        if modality == "video" and self.encode_formats.get("audio", None):
            audio_format = self.encode_formats["audio"]
            audio_path = video2audio(modality_paths["video"], audio_format, self.tmp_dir)
            if audio_path is not None:
                modality_paths["audio"] = audio_path

        for modality, modality_path in modality_paths.items():
            if modality not in self.encode_formats:
                os.remove(modality_path)
                modality_path.pop(modality)

        return modality_paths, None


class YtDlpDownloader:
    """Downloader class for yt-dlp links

    yt_args:
        download_size: preferred height of video to download. Will try to download smallest video >=download_size
        download_audio_rate: same as size but with audio
        yt_metadata_args: see get_yt_metadata function docstring
    """

    # TODO: maybe we just include height and width in the metadata_args
    def __init__(self, yt_args, tmp_dir, encode_formats, cookies_file=None):
        self.metadata_args = yt_args.get("yt_metadata_args", {})
        self.video_size = yt_args.get("download_size", 360)
        self.audio_rate = yt_args.get("download_audio_rate", 44100)
        self.tmp_dir = tmp_dir
        self.encode_formats = encode_formats

        if isinstance(cookies_file, str):
            self.cookie_files = [c.strip() for c in cookies_file.split(",") if c.strip()]
        elif isinstance(cookies_file, list):
            self.cookie_files = list(cookies_file)
        elif cookies_file:
            self.cookie_files = [cookies_file]
        else:
            self.cookie_files = []

        self.cookie_index = 0

        # TODO: figure out when to do this
        # was relevant with HD videos for loading with decord
        self.specify_codec = False

    def __call__(self, url):
        import re  # ensure regex module is available in multiprocessing context

        modality_paths = {}
        clip_span = None

        match = re.match(r"^([\w-]{11})_(\d{6})_(\d{6})$", url)
        if match:
            video_id, s, e = match.groups()
            clip_span = (int(s), int(e))
            url = f"https://www.youtube.com/watch?v={video_id}"

        video_format_string = (
            f"wv*[height>={self.video_size}][ext=mp4]{'[codec=avc1]' if self.specify_codec else ''}/"
            f"w[height>={self.video_size}][ext=mp4]{'[codec=avc1]' if self.specify_codec else ''}/"
            f"bv/b[ext=mp4]{'[codec=avc1]' if self.specify_codec else ''}"
        )
        audio_fmt_string = (
            f"wa[asr>={self.audio_rate}][ext=m4a] / ba[ext=m4a]" if self.audio_rate > 0 else "ba[ext=m4a]"
        )

        cookie_file = None
        if self.cookie_files:
            cookie_file = self.cookie_files[self.cookie_index % len(self.cookie_files)]
            self.cookie_index += 1

        if self.encode_formats.get("audio", None):
            audio_path_m4a = f"{self.tmp_dir}/{str(uuid.uuid4())}.m4a"
            ydl_opts = {
                "outtmpl": audio_path_m4a,
                "format": audio_fmt_string,
                "quiet": True,
            }
            if cookie_file:
                ydl_opts["cookiefile"] = cookie_file
            if clip_span is not None:
                ydl_opts["download_ranges"] = download_range_func(None, [(clip_span[0], clip_span[1])])
                ydl_opts["force_keyframes_at_cuts"] = True

            err = None
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ret_code = ydl.download(url)
                if ret_code != 0 or not os.path.exists(audio_path_m4a):
                    err = f"yt-dlp exited with status {ret_code}"
            except Exception as e:  # pylint: disable=(broad-except)
                err = str(e)
            if err is not None:
                print(f"video2dataset error: {err}")
                if os.path.exists(audio_path_m4a):
                    os.remove(audio_path_m4a)
            else:
                # TODO: look into this, don't think we can just do this
                # TODO: just figure out a way to download the preferred extension using yt-dlp
                # audio_path = audio_path_m4a.replace(".m4a", f".{self.encode_formats['audio']}")
                audio_path = audio_path_m4a
                modality_paths["audio"] = audio_path

        if self.encode_formats.get("video", None):
            video_path = f"{self.tmp_dir}/{str(uuid.uuid4())}.mp4"
            ydl_opts = {
                "outtmpl": video_path,
                "format": video_format_string,
                "quiet": True,
                "no_warnings": True,
            }
            if cookie_file:
                ydl_opts["cookiefile"] = cookie_file
            if clip_span is not None:
                ydl_opts["download_ranges"] = download_range_func(None, [(clip_span[0], clip_span[1])])
                ydl_opts["force_keyframes_at_cuts"] = True

            err = None
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ret_code = ydl.download(url)
                if ret_code != 0 or not os.path.exists(video_path):
                    err = f"yt-dlp exited with status {ret_code}"
            except Exception as e:  # pylint: disable=(broad-except)
                err = str(e)
            if err is not None:
                print(f"video2dataset error: {err}")
                if os.path.exists(video_path):
                    os.remove(video_path)
            else:
                modality_paths["video"] = video_path

        err = None
        try:
            if self.metadata_args:
                yt_meta_dict = get_yt_meta(url, self.metadata_args)
            else:
                yt_meta_dict = {}
        except Exception as e:  # pylint: disable=(broad-except)
            err = str(e)
            yt_meta_dict = {}

        if clip_span is not None:
            yt_meta_dict["clips"] = [[clip_span[0], clip_span[1]]]

        return modality_paths, yt_meta_dict, None


class VideoDataReader:
    """Video data reader provide data for a video"""

    def __init__(self, encode_formats, tmp_dir, reading_config, cookies_file=None):
        self.webfile_downloader = WebFileDownloader(reading_config["timeout"], tmp_dir, encode_formats)
        cookies = cookies_file if cookies_file is not None else reading_config.get("cookies_file")
        self.yt_downloader = YtDlpDownloader(reading_config["yt_args"], tmp_dir, encode_formats, cookies)

    def __call__(self, row):
        key, url = row

        meta_dict = None
        try:
            # TODO: make nice function to detect what type of link we're dealing with
            if get_file_info(url):  # web file that can be directly downloaded
                modality_paths, error_message = self.webfile_downloader(url)
            else:
                modality_paths, meta_dict, error_message = self.yt_downloader(url)
        except Exception as e:  # pylint: disable=(broad-except)
            modality_paths, meta_dict, error_message = {}, None, str(e)

        streams = {}
        for modality, modality_path in modality_paths.items():
            with open(modality_path, "rb") as modality_file:
                streams[modality] = modality_file.read()
            os.remove(modality_path)

        return key, streams, meta_dict, error_message
