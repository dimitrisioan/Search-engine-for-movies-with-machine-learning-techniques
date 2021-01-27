import sys
import json
import pprint as pp
import pandas as pd
from tabulate import tabulate
from elasticsearch import Elasticsearch
from sklearn.cluster import KMeans
from sklearn.preprocessing import OneHotEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
import ast
import math
import numpy as np
import pickle

ratings_df = pd.read_csv("ratings.csv")
clusters = pd.read_csv("clusters.csv")
movies_df = pd.read_csv("movies.csv")
movies_df['genres'] = movies_df['genres'].map(lambda x: x.split('|'))

print(ratings_df)
print(movies_df)

user_ids = ratings_df['userId'].unique()

all_genres = set()
for i, row in movies_df.iterrows():
    all_genres.update(row['genres'])

clusters_dict = []
for user in user_ids:
	for genre in all_genres:
	    clusters_dict.append(
	        {
	            'userId': user,
	            'genre': genre,
	            'avg': 0.0
	        }
	    )

# clusters = pd.DataFrame(clusters_dict)

# for user in user_ids:
# 	ratings = []
# 	# get users ratings
# 	u_ratings = ratings_df.loc[ratings_df['userId'] == user]
# 	for index, rating in u_ratings.iterrows():
# 		movie = rating['movieId'].astype(int)
# 		genres = movies_df.loc[movies_df['movieId'] == movie]['genres'].reset_index()
# 		genres = genres['genres'][0]
# 		for genre in genres:
# 			ratings.append(
# 				{
# 					'genre': genre,
# 					'rating': rating['rating']
# 				}
# 			)

# 	for genre in all_genres:
# 		ratings_ = list()
# 		sum = 0
# 		count = 0
# 		for rating in ratings:
# 			if rating.get('genre') == genre:
# 				sum += rating.get('rating')
# 				count += 1
# 		if count != 0:
# 			avg = sum / count
# 			clusters.loc[(clusters['userId'] == user) & (clusters['genre'] == genre), 'avg'] = avg
# 			print(clusters.loc[(clusters['userId'] == user) & (clusters['genre'] == genre)])
# clusters.to_csv(r'clusters.csv')

del clusters['index']
del clusters['genre']

sparce_matrix = clusters.values

sparce_matrix = np.split(sparce_matrix[:,1], np.unique(sparce_matrix[:, 0], return_index=True)[1][1:])

num_of_clusters = 2*len(all_genres)
kmeans = KMeans(n_clusters=num_of_clusters).fit(sparce_matrix)
clusters_of_users = kmeans.predict(sparce_matrix)


clusters_of_users_map = {k: [] for k in range(0, num_of_clusters)}
for c in range(0, num_of_clusters):
	users_on_the_cluster = []
	for user_id, cluster in enumerate(clusters_of_users):
		if cluster == c:
			users_on_the_cluster.append(user_id)
			clusters_of_users_map[c].append(user_id)

print(clusters_of_users_map)
pickle.dump(clusters_of_users_map, open("cluster.pkl", "wb"))
pickle.dump(clusters_of_users, open("user_cluster.pkl", "wb"))