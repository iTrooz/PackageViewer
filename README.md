# PackageViewer database

This repository contains scripts to create and manage a SQLite database of package metadata across Linux ditributions.

# What is this ?

All Linux distributions (Debian, Ubuntu, ArchLinux, Fedora..) have repositories that contains packages installable directly with the package manager. In these archives you can find the actual package, but also metadata about it: name, version, dependencies, files provided... The purpose of this project is to compile this metadata across multiple Linux distributions in a single SQLite database

The scripts handle downloading from archives, and compiling a database from multiple repositories specified in a [configuration file](config.yml)

The archive formats currently supported are the ones used by apt, dnf and pacman

# How do I use it ?
- Modify the [configuration file](config.yml) to your needs
- Run `./data_downloader_cli.py` to download all the metadata to `archives/`
- Use `data_manager_cli.py` to create and import stuff in the database. Example syntax: `./data_manager_cli.py add -d ubuntu -v 22.04`
- (Optional, for speed) Run `./data_manager_cli.py add-indexes` to add indexes. This roughly double the database size
- The output database should be in the file `out.db` in the project's root

# Use cases
- "What package installs command 'xyz' ?"
- "What are the files provided by this package ?"
- "On which distributions does this package exist ? Where is it the most recent ?"
- "What is the dependency graph of this package ?"
- Helps to match packages functionnalities match across distributions when they are not 1:1 ?

# Licence
The licence **for the scripts** is GPL-2-or-later
