import os
import tmdbsimple as tmdb
import csv 

tmdb.API_KEY = os.environ.get('api_key')

def getGenre():
    
    genres = tmdb.Genres()
    genres_list = genres.movie_list()
    fieldnames = genres_list.get('genres')[0].keys()
    
    with open('data/d_genres.csv', 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        
        print(genres_list)
        
        for genre in genres_list.get('genres'):
            writer.writerow(genre)

if __name__ == '__main__':
    getGenre()
