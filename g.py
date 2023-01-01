import gzip
from tqdm import tqdm

file = gzip.open("archives/ubuntu-22/jammy/Contents-amd64.gz", "rb")
for line in tqdm(file):
    print(line.decode().strip())