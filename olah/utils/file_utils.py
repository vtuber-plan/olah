

import os


def make_dirs(path: str):
    if os.path.isdir(path):
        save_dir = path
    else:
        save_dir = os.path.dirname(path)
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)