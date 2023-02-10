import yaml
import os
import itertools
import asyncio
import aiohttp
import aiofiles
import tqdm

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
    def __init__(self, conf_filepath) -> None:
        self.conf_filepath = conf_filepath

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
                url_md["subrepos"] = repo_obj.get("subrepos") # Only apt-based dists have this

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
                    for subrepo in repo.md["subrepos"]:
                        yield [
                            os.path.join(repo.archive_url, subrepo, 'binary-amd64', 'Packages.gz'),
                            os.path.join(base_uri, subrepo, 'Contents-amd64.gz')
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

    async def _query_download_size(self, files):

        client = aiohttp.ClientSession()
        tasks = []
        for url, file in files:
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
        
        return total
        
    async def _download_single_file(self, bar, session, url, file):
        MAX_CHUNK_SIZE = 2**16

        resp = await session.get(url)

        size = int(resp.headers["Content-Length"])
        bar.total += size

        async with aiofiles.open(file, "+wb") as f:
            async for chunk in resp.content.iter_chunked(MAX_CHUNK_SIZE):
                await f.write(chunk)
                bar.update(len(chunk))

    async def _download_files(self, files):
        # Create directory structure in advance
        for url, file in files:
            os.makedirs(os.path.dirname(file), exist_ok=True)
        
        # Prepare progress bar
        bar = tqdm.tqdm(total=0, unit='iB', unit_scale=True, unit_divisor=1024)

        # download files    
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url, file in files:
                if os.path.exists(file):
                    if input(f"Something exists at location {file}. Do you want to overwrite it ?") == 'n':
                        print("Skipping")
                        continue
                tasks.append(self._download_single_file(bar=bar, session=session, url=url, file=file))

            await asyncio.gather(*tasks)
        

    async def process(self):
        repos = itertools.chain(*self._get_repos())
        files = list(self._get_files(repos))
        
        print(f"Files to download: {len(files)}")

        mib_total = bytes_to_mib(await self._query_download_size(files))
        print(f'Total to download: {mib_total}MiB')

        await self._download_files(files)

        
    def _get_repos(self):
        f = yaml.safe_load(open(self.conf_filepath))
        
        for dist_name, obj in f.get("dists").items():
            yield self._get_dist_repos(dist_name, obj)