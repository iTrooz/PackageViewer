import os
import sqlite3

import timer
from packageviewer.parsers.apt_parser import AptParser

parser = AptParser("ubuntu", "22", None)

files_gen = parser._parse_files_file_("archives/ubuntu/22/security/Contents-amd64.gz", "security")

timer.start("gen files")
files = list(files_gen)
timer.stop()

DB_FILENAME = "/tmp/ram/out.db"

if os.path.exists(DB_FILENAME):
    os.remove(DB_FILENAME)

conn = sqlite3.connect(DB_FILENAME)
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

def way1(): # 38.6s
    conn.executescript('''CREATE TEMPORARY TABLE tmp_file(
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

def way2(): # 36.6s
    dirnames = {}
    filenames = {}

    timer.start("all insertions")
    for file in files:
        filename = file["filename"]
        filename_id = filenames.get(filename)
        if not filename_id:
            cursor = conn.execute("INSERT INTO filename (filename) VALUES (?)", (filename,))
            filename_id = cursor.lastrowid
            filenames[filename] = cursor.lastrowid

        dirname = file["dirname"]
        dirname_id = dirnames.get(dirname)
        if not dirname_id:
            cursor = conn.execute("INSERT INTO dirname (dirname) VALUES (?)", (dirname,))
            dirname_id = cursor.lastrowid
            dirnames[dirname] = cursor.lastrowid

        cursor = conn.execute("INSERT INTO file (dirname_id, filename_id) VALUES (?, ?)", (dirname_id, filename_id))
    timer.stop()

def way3(): # 9.5h calculé
    timer.start("all insertions")
    for file in files:
        filename = file["filename"]
        cursor = conn.execute("SELECT filename_id FROM filename WHERE filename = ?", (filename,))
        row = cursor.fetchone()
        if row == None:
            cursor = conn.execute("INSERT INTO filename (filename) VALUES (?)", (filename,))
            filename_id = cursor.lastrowid
        else:
            filename_id = row[0]

        dirname = file["dirname"]
        cursor = conn.execute("SELECT dirname_id FROM dirname WHERE dirname = ?", (dirname,))
        row = cursor.fetchone()
        if row == None:
            cursor = conn.execute("INSERT INTO dirname (dirname) VALUES (?)", (dirname,))
            dirname_id = cursor.lastrowid
        else:
            dirname_id = row[0]

        conn.execute("INSERT INTO file (dirname_id, filename_id) VALUES (?, ?)", (dirname_id, filename_id))
    timer.stop()

way1()

conn.commit()