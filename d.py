import database
from tqdm import tqdm
from time import time

database.reset_db()
db_session = database.get_session()

print("Creating inserts..")
start = time()
inserts = [{"package_name": "A", "filepath": "B"} for i in tqdm(range( (10**6) ))]
end = time()
print(f"Created inserts! {(end-start):.4f}s")

print("Inserting..")
start = time()
db_session.bulk_insert_mappings(
    database.PackageFile, inserts 
)
end = time()
print(f"Inserted! {(end-start):.4f}s")

print("Committing..")
start = time()
db_session.commit()
end = time()
print(f"Commited! {(end-start):.4f}s")