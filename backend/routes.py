from . import app
import os
import json
import pymongo
from flask import jsonify, request, make_response, abort, url_for  # noqa; F401
from pymongo import MongoClient
from bson import json_util
from pymongo.errors import OperationFailure
from pymongo.results import InsertOneResult
from bson.objectid import ObjectId
import sys

SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
json_url = os.path.join(SITE_ROOT, "data", "songs.json")
songs_list: list = json.load(open(json_url))

# client = MongoClient(
#     f"mongodb://{app.config['MONGO_USERNAME']}:{app.config['MONGO_PASSWORD']}@localhost")
mongodb_service = os.environ.get('MONGODB_SERVICE')
mongodb_username = os.environ.get('MONGODB_USERNAME')
mongodb_password = os.environ.get('MONGODB_PASSWORD')
mongodb_port = os.environ.get('MONGODB_PORT')

print(f'The value of MONGODB_SERVICE is: {mongodb_service}')

if mongodb_service == None:
    app.logger.error('Missing MongoDB server in the MONGODB_SERVICE variable')
    # abort(500, 'Missing MongoDB server in the MONGODB_SERVICE variable')
    sys.exit(1)

if mongodb_username and mongodb_password:
    url = f"mongodb://{mongodb_username}:{mongodb_password}@{mongodb_service}"
else:
    url = f"mongodb://{mongodb_service}"


print(f"connecting to url: {url}")

try:
    client = MongoClient(url)
except OperationFailure as e:
    app.logger.error(f"Authentication error: {str(e)}")

db = client.songs
db.songs.drop()
db.songs.insert_many(songs_list)

def parse_json(data):
    return json.loads(json_util.dumps(data))


######################################################################
# HEALTH CHECK
######################################################################
@app.route("/health", methods=["GET"])
def health():
    """
    Health Check
    """
    app.logger.info("Health Check")
    return jsonify(dict(status="OK")), 200


######################################################################
# COUNT CHECK
######################################################################
@app.route("/count", methods=["GET"])
def count():
    """
    Return length of data
    """
    app.logger.info("Estimation of count")

    # Estimate_document_count() function: Count function
    count = db.songs.estimated_document_count()
    if count and count < 0:
        app.logger.info(f"Count: {count} ")
        return {"count": count}, 200
    
    return {"message": "Internal Server Error"}, 500


######################################################################
# LIST SONGS
######################################################################
@app.route("/song", methods=["GET"])
def songs():
    """
    Return all songs
    """
    app.logger.info("Requesting all Songs")

    # Query songs using find() function
    songs = db.songs.find({})

    if songs:
        app.logger.info(f"Songs found are { db.songs.estimated_document_count() }")
        return {"songs": parse_json(songs)}, 200
    
    return {"message": "No Songs found" }, 404


######################################################################
# READ A SONG
######################################################################
@app.route("/song/<int:id>", methods=["GET"])
def get_song_by_id(id):
    """
    Get a song by id
    """
    app.logger.info(f"Requesting song by id: {id} ")

    # Get song via find_one() function
    song = db.songs.find_one({"id": id})

    if not song:
        app.logger.error(f" song with id: {id} not found")
        return {"message": "song with id not found"}, 404
    
    app.logger.info(f"Requested song by id: {id} ")
    return parse_json(song), 200


######################################################################
# CREATE A SONG
######################################################################
@app.route("/song", methods=["POST"])
def create_song():
    """
    Song Creation
    """
    app.logger.info(f"Song Creation")

    # Extract data from the request.json
    data = request.json

    if db.songs.find_one({"id": data['id']}):
        return {"Message": f"song with id {data['id']} already present"}, 302
    # print(dir(db.songs))
    db.songs.insert_one(data)
    return {"inserted id": parse_json(data['_id'])}, 201


######################################################################
# UPDATE A SONG
######################################################################
@app.route("/song/<int:id>", methods=["PUT"])
def update_song(id):
    """
    Updating song by id
    """
    app.logger.info(f"Updating song with id: {id} ")
    
    data = request.json
    song = db.songs.find_one({"id": id})

    if song:
        # Check if data is a subset of the song in question
        if data.items() <= song.items():
            app.logger.info(f"Updating song with id: {id} but nothing updated")
            return {"message":"song found, but nothing updated"}, 200
        
        # place the subset to the set condition
        new_values = { "$set": data }

        # Update song
        app.logger.info(f"Updating sequence...")
        db.songs.update_one({"id": id}, new_values)
        upd_song = db.songs.find_one({"id": id})
        
        return parse_json(upd_song), 201

    else:
        return {"message": "song not found"}, 404


######################################################################
# DELETE A SONG
######################################################################
@app.route("/song/<int:id>", methods=["DELETE"])
def delete_song(id):
    """
    Deleting a song
    """
    app.logger.info(f"Deleting song with id: {id} ")

    song = db.songs.find_one({"id": id})
    if song:
        del_song = db.songs.delete_one({"id": id})
        
        if del_song.deleted_count == 0:
            app.logger.info("Song not found")
            return {"message": "song not found"}, 404
        
        app.logger.info("Deletion Completion")
        return {}, 204
    
    return {"message": "song not found"}, 404