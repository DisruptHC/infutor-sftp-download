import os
import errno
import shutil

def is_zip_file(filename):
    return filename.endswith("zip")

def is_pgp_file(filename):
    return filename.endswith("gpg")

def remove_prefix(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix):]
    return string

def mkdir(dir):
    try:
        os.makedirs(dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def _rm_file(src):
    try:
        os.remove(src)
    except OSError as e:
        print("Error: %s : %s" % (src, e.strerror))

def _rm_dir(src):
    try:
        shutil.rmtree(src)
    except OSError as e:
        print("Error: %s : %s" % (src, e.strerror))

def rm(src):
    if os.path.isfile(src):
        _rm_file(src)
    if os.path.isdir(src):
        _rm_dir(src)

def rename_with_date(filepath, today):
    split_path = filepath.split('.', 1)
    return f"{split_path[0]}-{today}.{split_path[1]}"