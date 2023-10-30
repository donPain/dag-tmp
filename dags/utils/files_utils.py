import os

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


# def get_all_files_by_type(path, type):
    