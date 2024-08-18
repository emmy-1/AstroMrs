from dotenv import load_dotenv
import os
import requests
import json
from pymongo import MongoClient

load_dotenv()

api_url = os.getenv('API_URL')
authorization_key = os.getenv('AUTHORIZATION_KEY')

def fetch_movies(endpoint):
    all_movies = []
    
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {authorization_key}"
    }

    for page in range(1, 501):
        page_url = f"{api_url}{endpoint}?language=en-US&page={page}"
        response = requests.get(page_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            movies = data.get('results', [])
            all_movies.extend(movies)

        else:
            print(f"Failed to fetch page {page}: {response.status_code}")
            break

    return all_movies

def fetch_popular_movies():
    return fetch_movies('/movie/popular')

def fetch_top_rated_movies():
    return fetch_movies('/movie/top_rated')

"""
def save_movies_json(movies, filename):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(movies, file, ensure_ascii=False, indent=4)
    print(f"Movies saved to {filename}")
"""

def save_movies_mongo(movies, collection_name):
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['movie_database']
    collection = db[collection_name]

    if movies:
        collection.insert_many(movies)
        print(f"Movies saved to MongoDb Collection {collection_name}")
    else:
        print(f"No movies to save to MongoDB collection '{collection_name}'")




popular_movies = fetch_popular_movies()
top_rated_movies = fetch_top_rated_movies()

#save_movies_json(popular_movies, 'popular_movies.json')
#save_movies_json(top_rated_movies, 'top_rated_movies.json')

save_movies_mongo(popular_movies, 'popular_movies')
save_movies_mongo(top_rated_movies, 'top_rated_movies')

print(f"Total popular movies fetched: {len(popular_movies)}")
print(f"Total top rated movies fetched: {len(top_rated_movies)}")
