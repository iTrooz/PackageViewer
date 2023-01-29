import os
import sqlite3

import timer
from packageviewer.parsers.apt_parser import AptParser

parser = AptParser("ubuntu", "22", None)

files_gen = parser._parse_files_file_("archives/ubuntu/22/security/Contents-amd64.gz", "security")

timer.start("gen files")
files = list(files_gen)
timer.stop()

if os.path.exists("/tmp/ram/out.db"):
    os.remove("/tmp/ram/out.db")

conn = sqlite3.connect("/tmp/ram/out.db")
conn.executescript('''CREATE TABLE file(
            dirname_id INTEGER,
            filename_id INTEGER
        );
        CREATE TABLE dirname(
            dirname_id INTEGER PRIMARY KEY AUTOINCREMENT,
            dirname
        );
        CREATE TABLE filename(
            filename_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT
        );''')

def way1(): # 37.7s
    conn.executescript('''CREATE TABLE tmp_file(
        package, repo, dirname, filename);
    ''')

    timer.start("tmp_file")
    conn.executemany("INSERT INTO tmp_file VALUES (?, ?, ?, ?)", (list(file.values()) for file in files))
    timer.stop()

    timer.start("dirname")
    conn.execute('''INSERT INTO dirname (dirname)
    SELECT DISTINCT dirname FROM tmp_file
    ''')
    timer.stop()

    timer.start("filename")
    conn.execute('''INSERT INTO filename (filename)
    SELECT DISTINCT filename FROM tmp_file
    ''')
    timer.stop()


    timer.start("file")
    conn.execute('''INSERT INTO file (dirname_id, filename_id)
    SELECT dirname.dirname_id, filename.filename_id FROM tmp_file
    JOIN filename ON filename.filename = tmp_file.filename
    JOIN dirname ON dirname.dirname = tmp_file.dirname
    ''')
    timer.stop()

def way2():
    dirnames = {}
    filenames = {}

    timer.start("all insertions")
    for file in files:
        filename = file["filename"]
        if not filename in filenames:
            cursor = conn.execute("INSERT INTO filename (filename) VALUES (?)", (filename,))
            filename_id = cursor.lastrowid
            filenames[filename] = cursor.lastrowid
        else:
            filename_id = filenames[filename]

        dirname = file["dirname"]
        if not dirname in dirnames:
            cursor = conn.execute("INSERT INTO dirname (dirname) VALUES (?)", (dirname,))
            dirname_id = cursor.lastrowid
            dirnames[dirname] = cursor.lastrowid
        else:
            dirname_id = dirnames[dirname]

        cursor = conn.execute("INSERT INTO file (dirname_id, filename_id) VALUES (?, ?)", (dirname_id, filename_id))
    timer.stop()

way2()

conn.commit()