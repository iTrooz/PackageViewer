import database
from tqdm import tqdm
from time import time
from sqlalchemy import insert

database.reset_db()
db_session = database.get_session()

print("Creating inserts..")
start = time()
inserts = [
    database.Package(distro_name="A",distro_version="A",distro_repo="A",name="A",version="A",arch="A",others={"A":"A","B":"B","C":"C","D":"D","E":"E"})
    for i in tqdm(range( (10**6) ))]
end = time()
print(f"Created inserts! {(end-start):.4f}s")

print("Inserting..")
start = time()
db_session.add_all(inserts)
end = time()
print(f"Inserted! {(end-start):.4f}s")

print("Committing..")
start = time()
db_session.commit()
end = time()
print(f"Commited! {(end-start):.4f}s")