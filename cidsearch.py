import pandas as pd
import hashlib
import pymongo
import json
import sys

# config
with open("uri.txt") as f:
    uri = f.read()
#filename = 'deaths.csv'
#src = sys.argv[1]
#dest = sys.argv[2]

src = input("Enter source file  path (CSV)...\n")
dest = input("Enter destination file path (CSV)...\n")


print(f"\nSource file: {src}")
print(f"\nDestination file: {dest}")

df = pd.read_csv(src, dtype=str)

print(df.iloc[:2])
cid = input("\nEnter cid column name:\n")

print("Hashing CID (MD5)...")
df_notnull = df[df[cid].notnull()]
df_notnull['cid_hash'] = df_notnull[cid].str.encode('utf8').map(lambda x: hashlib.md5(x).hexdigest()).str.upper() + ':' + df_notnull[cid].str.slice(stop=1) + df_notnull[cid].str.slice(start=-1)
cid_hash = df_notnull['cid_hash'].dropna().drop_duplicates().tolist()


df = df.merge(df_notnull.set_index(cid)[['cid_hash']], left_on=cid, right_on=cid, how='left')


print(cid_hash)

print("Connecting to MongoDB...")
client = pymongo.MongoClient(uri)
db = client.moph_immunization_center

match = {
    "$match": {
        "cid": {"$in": cid_hash } 
        } 
    }

fields = {
    "$project": {
        "_id": 0, 
        "cid": 1, 
        "immunization_date": {
            "$dateToString": {
                "date": "$immunization_date",
                "format": "%Y-%m-%d"
            }
        }, 
        "vaccine_manufacturer": 1
        }
    }

sort = {
    "$sort": {
        "cid": 1,
        "immunization_date": 1
    }
}

print("Query data from MongoDB...")
query = db.visit_immunization.aggregate([match, fields, sort])
vac = pd.DataFrame(query)

print("Transforming data...")
vac['dose'] = vac.groupby('cid').cumcount() + 1

vac = vac.set_index(['cid', 'dose'])
vac.columns.name = 'vaccines'
vac_wide = vac.stack().unstack(['dose', 'vaccines'])
vac_wide.columns = [f"{column[1]}_{column[0]}" for column in vac_wide.columns]
vac_wide['num_dose'] = vac.reset_index().groupby('cid')['dose'].count()

# merge vac_wide with original data
print(f"Saving data to {dest}...")
result = df.merge(vac_wide, left_on='cid_hash', right_index=True, how='left').drop(columns='cid_hash')
result.to_csv(dest, index=False, encoding='utf-8-sig')

client.close()
