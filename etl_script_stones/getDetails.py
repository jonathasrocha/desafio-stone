from requests.exceptions import HTTPError
from movies.Movies import Movies 
import tmdbsimple as tmdb
import os
import csv
import json

tmdb.API_KEY = os.environ.get('api_key')

def getUniqueMovies():

    # Cria a lista unicas de filmes
    movies_list = []
    movies = tmdb.Movies()

    # Le os filmes em cartaz e em lancamento
    with  open('data/d_movie_nowplaying.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader, None)
        for movie in reader:
            if movie[0] not in movies_list:
                movies_list.append(movie[0])

    with  open('data/d_movie_upcoming.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader, None)
        for movie in reader:
            if movie[0] not in movies_list:
                movies_list.append(movie[0])
    return movies_list

if __name__ == '__main__':
    movies = Movies()
    movies_list = getUniqueMovies()
    movies.getDetailFromListMovie(movies_list)
