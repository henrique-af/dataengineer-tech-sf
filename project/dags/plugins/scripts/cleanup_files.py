from datetime import datetime
import logging
import os


def cleanup_files(input_path: str, processing_path: str):
    logging.info("Starting cleanup of files.")
    today = datetime.now().date()
    for path in [input_path, processing_path]:
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            file_date_str = file_name.split("-")[-1].replace(".csv", "")
            try:
                file_date = datetime.strptime(file_date_str, "%Y-%m-%d").date()
                if file_date != today:
                    logging.info(f"Removing outdated file: {file_name}")
                    os.remove(file_path)
            except ValueError:
                logging.info(f"Removing processed file: {file_name}")
                os.remove(file_path)
    logging.info("Cleanup complete.")
