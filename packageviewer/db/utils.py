import os


def loop_dirs(base_dir):
    for subdir in os.listdir(base_dir):
        full_subdir = os.path.join(base_dir, subdir)
        if os.path.isdir(full_subdir):
            yield subdir, full_subdir


def ask(msg, default="y"):

    if default == "y":
        full_msg = f"{msg} [Y/n] "
    else:
        full_msg = f"{msg} [y/N] "

    answer = None
    while answer not in ("y", "n"):
        answer = input(full_msg).lower()
        if not answer:
            answer = default

    return answer
