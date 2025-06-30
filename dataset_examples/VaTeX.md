# [VaTeX](https://eric-xw.github.io/vatex-website/)

VaTeX is a multilingual video caption dataset. The metadata can be downloaded as a JSON file and processed with video2dataset. The `examples/download_vatex_modal.py` script demonstrates how to download VaTeX on Modal and upload the processed dataset to the Hugging Face Hub. It supports providing multiple `cookies.txt` paths for rotating cookies when fetching YouTube videos.

Run the script locally with:

```bash
python examples/download_vatex_modal.py path1.txt,path2.txt username/vatex
```

This uses Modal to launch a job that downloads the JSON metadata, clones video2dataset, processes the videos using rotating cookies, and finally uploads the result to Hugging Face.
