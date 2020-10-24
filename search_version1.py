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



if __name__ == '__main__':
    while(True):
        print('1--Search for a movie')
        print('2--Exit')
        decision = int(input('Select from menu: '))
        if(decision == 1):
            print('What are you looking for?')
            search_term = input()
            query = {
                        "from" : 0, 
                        "size" : 20,
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
            exit(0)
        else:
            continue
