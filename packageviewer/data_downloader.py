#!/usr/bin/env python3
import yaml
import os
import itertools
import asyncio
import aiohttp
import aiofiles
import tqdm

from packageviewer.utils import ask

def bytes_to_mib(bytes_n):
    return round(bytes_n/(1024*1024), 2)
class RepoData:
    def __init__(self, archive_url, md) -> None:
        self.archive_url = archive_url
        self.md = md

    def process_archive_url(self):
        for key, value in self.md.items():
            if type(value) == str:
                self.archive_url = self.archive_url.replace("$"+key, value)

        if "$" in self.archive_url:
            print("Warning: found unprocessed variable in archive url "+self.archive_url)

    def __str__(self):
        return f"RepoData[archive_url={self.archive_url}, md={self.md}]"

    def __repr__(self):
        return self.__str__()

class DataDownloader:
    def __init__(self, config_path, output_dir, force) -> None:
        self.config_path = config_path
        self.output_dir = output_dir
        self.force = force

    def _get_dist_repos(self, distro_name, dist_obj):
        distro_archive_url = dist_obj.get("archive")

        for version in dist_obj.get("versions", [{}]):
            for repo_name, repo_obj in dist_obj.get("repos").items():
                url_md = {}
                if not repo_obj.get("version_agnostic"):
                    url_md.update(version)
                url_md["pm_type"] = dist_obj["pm_type"]
                url_md["repo"] = repo_name
                url_md["distro"] = distro_name
                url_md["areas"] = repo_obj.get("areas") # Only apt-based dists have this

                archive_url = repo_obj.get("archive") or distro_archive_url

                archive = RepoData(archive_url, url_md)
                archive.process_archive_url()
                yield archive

    def _get_files(self, repos):
        for repo in repos:

            base_uri = os.path.join('archives',
                repo.md["distro"],
                repo.md.get("version_id") or "DEFVERSION",
                repo.md.get("repo") or "DEFREPO",
            )

            match repo.md["pm_type"]:
                case "apt":
                    yield [
                        os.path.join(repo.archive_url, 'Contents-amd64.gz'),
                        os.path.join(base_uri, 'Contents-amd64.gz')
                    ]
                    for area in repo.md["areas"]:
                        yield [
                            os.path.join(repo.archive_url, area, 'binary-amd64', 'Packages.gz'),
                            os.path.join(base_uri, area, 'Packages.gz')
                        ]

                case "dnf":
                    yield [
                        os.path.join(repo.archive_url, 'repodata/repomd.xml'),
                        os.path.join(base_uri, 'repomd.xml')
                    ]
                case "pacman":
                    yield [
                        os.path.join(repo.archive_url, f'{repo.md["repo"]}.db'),
                        os.path.join(base_uri, f'{repo.md["repo"]}.db')
                    ]

    async def query_download_size(self):

        client = aiohttp.ClientSession()
        tasks = []
        for url, file in self.files:
            task = client.head(url, allow_redirects=True)
            tasks.append(task)
        
        total = 0
        results = await asyncio.gather(*tasks)
        for result in results:
            if result.ok:
                size = int(result.headers["Content-Length"])
                
                mib = bytes_to_mib(size)
                if mib > 5:
                    print(f"Big request: {result.url} : {mib}MiB")

                total += size
            else:
                print(f"Warning: request {result.url} returned HTTP code {result.status}")

        await client.close()

        self.total_download_size = total
        
        return total
        
    async def _download_single_file(self, bar, session, url, file):
        MAX_CHUNK_SIZE = 2**16

        resp = await session.get(url)

        if not resp.ok:
            print(f"Warning: request {resp.url} returned HTTP code {resp.status}")
            return
        
        # if not set, try to calculate it ourselves, but it won't be directly accurate, see warning message
        if not self.total_download_size:
            size = int(resp.headers["Content-Length"])
            bar.total += size

        async with aiofiles.open(file, "+wb") as f:
            async for chunk in resp.content.iter_chunked(MAX_CHUNK_SIZE):
                await f.write(chunk)
                bar.update(len(chunk))

    async def download_files(self):
        # Create directory structure in advance
        for url, file in self.files:
            os.makedirs(os.path.dirname(file), exist_ok=True)
        
        # Prepare progress bar
        bar = tqdm.tqdm(total=0, unit='iB', unit_scale=True, unit_divisor=1024)
        if self.total_download_size:
            bar.total = self.total_download_size
        else:
            print("query_download_size() not called. Total download size will not be accurate until all requests have been made (5 concurrently)")

        # download files    
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5)) as session:
            tasks = []
            for url, file in self.files:
                if os.path.exists(file):
                    if self.force or ask(f"Something exists at location {file}. Do you want to overwrite it ?", 'n') == 'n':
                        print(f"Skipping {file}")
                        continue
                tasks.append(self._download_single_file(bar=bar, session=session, url=url, file=file))

            await asyncio.gather(*tasks)
        

    def init(self):
        repos = itertools.chain(*self._get_repos())
        self.files = list(self._get_files(repos))

        
    def _get_repos(self):
        f = yaml.safe_load(open(self.config_path))
        
        for dist_name, obj in f.get("dists").items():
            yield self._get_dist_repos(dist_name, obj)