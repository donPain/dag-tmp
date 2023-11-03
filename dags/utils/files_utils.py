import os
import re

def cleanup_volume(file_path):
    print("Cleaning volume dir: " + file_path)
    for root, dirs, files in os.walk(file_path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))
        for name in dirs:
            sub_path = os.path.join(root, name)
            if not os.listdir(sub_path):
                os.rmdir(sub_path)
            else:
                print(f"A pasta {sub_path} não está vazia e não pode ser removida.")

def get_osc_file_paths(dir):
    all_files = os.listdir(dir)
    files = [file for file in all_files if file.endswith(".osc.gz")]
    sorted_files = sorted(files, key=lambda x: int(re.split(r'\D+', x)[0]))
    file_paths = [os.path.join(dir, file) for file in sorted_files]
    return file_paths