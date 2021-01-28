import sys
import json
import pprint as pp
import pandas as pd
from tabulate import tabulate
from elasticsearch import Elasticsearch
import ast
import numpy as np

filename = 'movies.csv'

es = Elasticsearch(
    ['localhost'],
    port=9200
)

INDEX = 'movies'
DOC_TYPE = 'movie'

user_weight = 1
avg_weight = 0.5

def convert_DataFrame_to_JSON(df):
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    json_str = json.dumps(parsed, indent=4)

    return json.loads(json_str)

def load_and_convert_CSV_to_JSON(filename):
    dataFrame2 = pd.read_csv("ratings.csv")

    result = dataFrame2.to_json(orient="records")
    parsed = json.loads(result)
    json_str = json.dumps(parsed, indent=4)

    return json.loads(json_str)

ratings = pd.read_csv("ratings.csv")

def searchMovies(query):
    response = es.search(index=INDEX, doc_type=DOC_TYPE, body=query)
    print("documents returned:",  len(response["hits"]["hits"]))

    # convert elastic search results to dataframe
    elastic_df = pd.DataFrame(response['hits']['hits'])

    # get the movies data in a separate dataframe
    _source = elastic_df[['_source']].copy()

    # convert movies data to dict
    js=json.loads(_source.to_json())

    # the the first value from the dict
    # contains all the movies data
    key, value = js.popitem()

    # convert them to a dataframe where each value is separate
    movies_object = pd.DataFrame.from_dict(value, orient='index')

    # add an index column based on the row index
    movies_object['index'] = movies_object.reset_index().index

    # add an index column based on the row index
    elastic_df['index'] = elastic_df.reset_index().index

    # merge the dataframes
    results = pd.merge(movies_object, elastic_df)

    # delete all the unnecessary columns
    del results['index']
    del results['_index']
    del results['_type']
    del results['_source']
    del results['_id']

    return results



ratings_df = pd.read_csv("ratings.csv")
movies_avg_ratings = ratings_df.groupby(['movieId'])['rating'].mean()

if __name__ == '__main__':
    while(True):
        print('1--Search for a movie')
        print('2--Exit')
        decision = int(input('Select from menu: '))
        if(decision == 1):
            requested_id = int(input('Please give your user_id:'))
            user_ratings = ratings_df.loc[ratings_df['userId'] == requested_id]
            user_ratings.rename(columns={'rating': 'user_rating'}, inplace=True)
            print('What are you looking for?')
            search_term = input()
            query = {
                "from": 0,
                "size": 10,
                "query":
                {
                    "simple_query_string":
                    {
                        "query": search_term,
                        "fields": ["title", "genres"]
                    }
                }
            }
            results = searchMovies(query)

            max_score = results.loc[results['_score'].idxmax()]._score

            results = pd.merge(results, movies_avg_ratings, on='movieId')
            results = pd.merge(results, user_ratings[['user_rating', 'movieId']], on='movieId', how='left').fillna(0)
            results.rename(columns={'rating': 'avg_rating'}, inplace=True)
            results['score'] = (results['avg_rating'] + user_weight * results['user_rating'] + avg_weight * results['_score'])/max_score
            results.sort_values(by = 'score', inplace=True, ascending=False)
            print(results)
        elif(decision == 2):
            exit(0)
        else:
            continue