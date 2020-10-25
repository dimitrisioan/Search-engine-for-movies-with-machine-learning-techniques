import sys
import json
import pprint as pp
import pandas as pd
from elasticsearch import Elasticsearch

filename = 'movies.csv'
es = Elasticsearch(
    ['localhost'],
    port=9200

)

def convert_DataFrame_to_JSON(df):
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    json_str = json.dumps(parsed, indent=4)

    return json.loads(json_str)

dataFrame = pd.read_csv("ratings.csv") 
# pd.set_option('display.max_rows = 100', None)
movies_avg_ratings = dataFrame.groupby(['movieId'])['rating'].mean()

# user_ratings = dataFrame.loc[dataFrame['userId'] == 1]

# json = convert_DataFrame_to_JSON(user_ratings)

# pp.pprint(json)

#TODO: Import ratings per movie on elastic search

#TODO: Create quere using the new score

