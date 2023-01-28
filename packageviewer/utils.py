import os

def loop_dirs(base_dir):
    for subdir in os.listdir(base_dir):
        full_subdir = os.path.join(base_dir, subdir)
        if os.path.isdir(full_subdir):
            yield subdir, full_subdir
