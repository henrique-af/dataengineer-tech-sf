import zipfile
import os
import logging


def unzip_file(input_path: str, processing_path: str):
    for file_name in os.listdir(input_path):
        if file_name.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input_path, file_name), "r") as zip_ref:
                zip_ref.extractall(processing_path)
            logging.info(f"FILE EXTRACTED: {file_name} in PATH: {processing_path}")
