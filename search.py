import sys
import json
import pandas as pd
from pprint import pprint
from elasticsearch import Elasticsearch

filename = 'movies.csv'
es = Elasticsearch(
    ['localhost'],
    port=9200

)

def load_and_convert_CSV_to_JSON(filename):
    dataFrame = pd.read_csv("movies.csv") 

    result = dataFrame.to_json(orient="records")
    parsed = json.loads(result)
    json_str = json.dumps(parsed, indent=4)  

    return json.loads(json_str)

def insert_movies_to_elasticsearch(object):
    counter = 0
    for movie in object:
        es.index(index='movies', doc_type='movie', id=counter, body=movie)
        counter += 1
    print('Movies inserted successfully')

if __name__ == "__main__":
    movies = load_and_convert_CSV_to_JSON(filename)
    insert_movies_to_elasticsearch(movies)