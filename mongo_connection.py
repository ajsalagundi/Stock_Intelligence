from pymongo import MongoClient

client = MongoClient('localhost', 27017)

db = client.test
print(db.stock_intell.insert_one({'x': 10}).inserted_id)
