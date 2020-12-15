import json
import requests
import pandas as pd
from tabulate import tabulate
from elasticsearch import Elasticsearch

es = Elasticsearch(
    ['localhost'],
    port=9200
)

INDEX = 'movies'
DOC_TYPE = 'movie'


def searchMovies(query):
    request = es.search(index=INDEX, doc_type=DOC_TYPE, body=query)
    dataFrame = pd.DataFrame(request['hits']['hits'])

    print("{} documents found".format(len(request['hits']['hits'])))
    print(tabulate(dataFrame, headers='keys', tablefmt='psql'))

def load_and_convert_CSV_to_JSON(filename):
    dataFrame2 = pd.read_csv("ratings.csv") 

    result = dataFrame2.to_json(orient="records")
    parsed = json.loads(result)
    json_str = json.dumps(parsed, indent=4)

    return json.loads(json_str)


if __name__ == '__main__':
    while(True):
        print('1--Search for a movie with the default metric(BM25).')
        print('2--Search for a movie with your custom metric.')
        print('3--Exit.')
        while(True):
                try:
                    decision = int(input('Select from menu: '))
                    
                except ValueError:
                    print('Sorry that is not an option ')
                    continue
                else:
                    break
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
            searchMovies(query)
        elif(decision == 2):
            #give user id and check if exists in ratings file
                requested_id = int(input('Please give your user_id:'))
                
                #find userid in ratings file check which movieid has userid 
                #check if user_id exitsis ratings field and and if yes ccontinue find userid in ratings file check which movieid has userid if not continue and reask user to give correct userid 
            #search for the movie and edit with the new metric
            
        elif(decision == 3):
            exit(0)
        
        else:
            print('Sorry, that is not an option try again!\n')
            continue







