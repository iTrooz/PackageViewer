#!/usr/bin/env python3

import asyncio
import argparse
import shutil
import os

from packageviewer.db.data_downloader import DataDownloader, bytes_to_mib
from packageviewer.db.utils import ask


class DataDownloaderCli:
    def __init__(self):
        self.SCRIPT_VERSION = "v1.0 beta"

    def run(self):
        parser = argparse.ArgumentParser(
            prog="data_downloader_cli",
            description="Download data",
            epilog=f"script version {self.SCRIPT_VERSION}",
        )

        parser.add_argument(
            "--config", "-i", default="config.yml", help="Set database file to use"
        )

        parser.add_argument(
            "--preview",
            "-p",
            default=False,
            action="store_true",
            help="Only preview download size and exit",
        )

        parser.add_argument(
            "--output_dir", "-o", default="archives", help="Set the distribution"
        )

        parser.add_argument(
            "--clear",
            default=False,
            action="store_true",
            help="Clear the output directory before downloading",
        )

        parser.add_argument(
            "--filter",
            required=False,
            help="Filter to only download some files. Syntax: distro/version/content . e.g: ubuntu/22.04",
        )

        parser.add_argument(
            "--force",
            "-f",
            default=False,
            action="store_true",
            help="Do not ask for confirmation to overwrite files",
        )

        self.args = parser.parse_args()

        asyncio.run(self.do())

    async def do(self):
        if self.args.clear:
            print(f"Deleting directory {self.args.output_dir}..")
            shutil.rmtree(self.args.output_dir)
            os.makedirs(self.args.output_dir)

        self.dd = DataDownloader(
            self.args.config, self.args.output_dir, self.args.force
        )

        print("Querying files to download..")
        await self.dd.init_files()

        if self.args.filter:
            print(f"Applying filter {self.args.filter}")
            split = self.args.filter.split("/")
            distro_version = None
            if len(split) == 1:
                distro_name = split[0]
                distro_version = None
            elif len(split) == 2:
                distro_name, distro_version = split
            else:
                raise ValueError("Invalid filter")

            self.dd.filter(distro_name, distro_version)

        print(f"Number of files to download: {len(self.dd.files)}")

        print("Querying download size..")
        mib_total = bytes_to_mib(await self.dd.query_download_size())
        print(f"Total to download: {mib_total}MiB")

        if self.args.preview:  # Only preview
            return

        if not self.args.force and ask("Do you want to continue ?", "y") == "n":
            return

        await self.dd.download_files()
        print("Download finished !")


cli = DataDownloaderCli()
cli.run()
