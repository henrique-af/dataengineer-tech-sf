import shutil
import os

def copy_files():
    source_dirs = {
        "project/dags": "dags",
        "project/plugins": "plugins",
    }

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


if __name__ == "__main__":
    copy_files()
