import database
from tqdm import tqdm
from time import time

database.reset_db()
db_session = database.get_session()

print("Creating inserts..")
start = time()
inserts = [{"package_name": "A", "filepath": "B"} for i in tqdm(range( (10**7) ))]
end = time()
print(f"Created inserts! {(end-start):.4f}s")

input()

print("Inserting..")
start = time()
for i in tqdm(range(0, len(inserts), 1_000_000)):
    db_session.bulk_insert_mappings(
        database.PackageFile, inserts[i:i+1_000_000]
    )
end = time()
print(f"Inserted! {(end-start):.4f}s")

input()

print("Committing..")
start = time()
db_session.commit()
end = time()
print(f"Commited! {(end-start):.4f}s")