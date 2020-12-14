import sys
import json
import pprint as pp
import pandas as pd
from tabulate import tabulate
from elasticsearch import Elasticsearch

filename = 'movies.csv'
es = Elasticsearch(
    ['localhost'],
    port=9200
)

INDEX = 'movies'
DOC_TYPE = 'movie'


user_weight = 2
users_weight = 1

def convert_DataFrame_to_JSON(df):
    result = df.to_json(orient="records")
    parsed = json.loads(result)
    json_str = json.dumps(parsed, indent=4)

    return json.loads(json_str)

def searchMovies(query):
    request = es.search(index=INDEX, doc_type=DOC_TYPE, body=query)
    dataFrame = pd.DataFrame(request['hits']['hits'])

    return dataFrame

    # print("{} documents found".format(len(request['hits']['hits'])))
    # print(tabulate(dataFrame, headers='keys', tablefmt='psql'))


dataFrame = pd.read_csv("ratings.csv") 
# pd.set_option('display.max_rows', None)
movies_avg_ratings = dataFrame.groupby(['movieId'])['rating'].mean()

print(movies_avg_ratings.loc[:5])



# user_ratings = dataFrame.loc[dataFrame['userId'] == 1]

# json = convert_DataFrame_to_JSON(user_ratings)

# pp.pprint(json)

#TODO: Import ratings per movie on elastic search

#TODO: Create query using the new score

# metric = el_score + user_weight * user_rating + users_weight * users_rating

if __name__ == '__main__':
    while(True):
        print('1--Search for a movie')
        print('2--Exit')
        decision = int(input('Select from menu: '))
        if(decision == 1):
            print('What are you looking for?')
            search_term = input()
            query = {
                "from": 0,
                "size": 20,
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
            new = tabulate(results, headers='keys', tablefmt='psql')
            print(results)

            # new = movies_avg_ratings.merge(results[['rating', '_score']], left_on='movieId', right_on='id')
            print(type(movies_avg_ratings))

        elif(decision == 2):
            exit(0)
        else:
            continue
