
### Here this code must be inside the folder(Dataset) containing the json data which is needed to be inserted into the MongoDb dataase
def add_video(collection,json_data):
    post_id = collection.insert_one(json_data).inserted_id;
    print(post_id);


def get_db():
    from pymongo import MongoClient
    client = MongoClient()  #Add authentication if your database is password protected.
    db = client.new_database; # Creates a new data base if not exits.
    return db


def get_insert_data():
	import json
	import glob	
	import os
	db = get_db();
	collection = db.test_collection; #Creates a new collection if not exists
	for filename in glob.glob('*.json'):
		print(filename);
		with open(filename) as json_file:		
			json_data= json.load(json_file);
			add_video(collection,json_data);


if __name__ == "__main__":
	get_insert_data();


    

