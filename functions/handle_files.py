import os
import shutil


def delete_folder_contents(folder_path: str):
    """
    Deletes all contents of a folder but keeps the folder itself.
    """
    if os.path.exists(folder_path):
        for item in os.listdir(folder_path):
            shutil.rmtree(os.path.join(folder_path, item), ignore_errors=True)
