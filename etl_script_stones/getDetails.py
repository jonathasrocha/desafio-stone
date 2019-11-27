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
    with  open('data/movies_nowplaying.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader, None)
        for movie in reader:
            if movie[4] not in movies_list:
                movies_list.append(movie[4])

    with  open('data/movies_upcoming.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader, None)
        for movie in reader:
            if movie[4] not in movies_list:
                movies_list.append(movie[4])
    return movies_list

if __name__ == '__main__':
    movies = Movies()
    movies_list = getUniqueMovies()
    movies.getDetailFromListMovie(movies_list)
