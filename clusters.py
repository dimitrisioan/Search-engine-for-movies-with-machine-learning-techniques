import sys
import json
import pprint as pp
import pandas as pd
from tabulate import tabulate
from elasticsearch import Elasticsearch
import ast
import numpy as np

ratings_df = pd.read_csv("ratings.csv") 
movies_df = pd.read_csv("movies.csv") 
movies_df['genres'] = movies_df['genres'].map(lambda x: x.split('|'))

print(ratings_df)
print(movies_df)

user_ids = ratings_df['userId'].unique()

genres = set()
for i, row in movies_df.iterrows():
    genres.update(row['genres'])

clusters = pd.DataFrame(columns=['user_ids', 'genre', 'avg'])

for user_id in user_ids:
    for genre in genres:
        row = [user_id, genre, 0.0]
        clusters.loc[len(clusters)] = row
        
print(clusters)

# user_id, avg_rating, genre

# gia kathe rating
#     if user & genre in df
#         update
#     else 
#         insert user, genre