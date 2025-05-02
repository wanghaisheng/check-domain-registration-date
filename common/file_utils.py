import os
import shutil
import zipfile

def ensure_dir_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def zip_folder(folder_path, output_folder, max_size_mb):
    ensure_dir_exists(output_folder)
    max_size_bytes = max_size_mb * 1024 * 1024
    zip_count = 1
    zip_temp_file = os.path.join(output_folder, f"temp{zip_count}.zip")
    zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)
    current_zip_size = 0
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            if current_zip_size + os.path.getsize(file_path) > max_size_bytes:
                zip_file.close()
                final_zip_path = os.path.join(output_folder, f"archive{zip_count}.zip")
                shutil.move(zip_temp_file, final_zip_path)
                current_zip_size = 0
                zip_count += 1
                zip_temp_file = os.path.join(output_folder, f"temp{zip_count}.zip")
                zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)
            zip_file.write(file_path)
            current_zip_size += os.path.getsize(file_path)
    zip_file.close()
    final_zip_path = os.path.join(output_folder, f"archive{zip_count}.zip")
    shutil.move(zip_temp_file, final_zip_path)

def read_lines(filename):
    with open(filename, 'r', encoding='utf8') as f:
        return f.read().splitlines()

def write_lines(filename, lines):
    with open(filename, 'w', encoding='utf8') as f:
        for line in lines:
            f.write(line + '\n') 