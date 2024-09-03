import shutil
import os
import zipfile
from datetime import datetime


def copy_files():
    source_dirs = {"project/dags": "dags"}

    for src, dst in source_dirs.items():
        if not os.path.exists(dst):
            os.makedirs(dst)

        for item in os.listdir(src):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)


def update_csv_file():
    input_folder = "dags/plugins/bucket/input"
    today = datetime.now().strftime("%Y-%m-%d")
    old_filename = f"mock_file_2024-09-03.csv"
    new_filename = f"mock_file_{today}.csv"

    old_filepath = os.path.join(input_folder, old_filename)
    new_filepath = os.path.join(input_folder, new_filename)

    if os.path.exists(old_filepath):
        os.rename(old_filepath, new_filepath)

        zip_filename = f"mock_file_{today}.zip"
        zip_filepath = os.path.join(input_folder, zip_filename)
        with zipfile.ZipFile(zip_filepath, "w") as zipf:
            zipf.write(new_filepath, os.path.basename(new_filepath))

        os.remove(new_filepath)


if __name__ == "__main__":
    copy_files()
    update_csv_file()
