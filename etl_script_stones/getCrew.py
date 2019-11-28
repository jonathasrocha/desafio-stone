import os
import csv
import tmdbsimple as tmdb
from requests.exceptions import HTTPError
from movies.Movies import Movies
tmdb.API_KEY = os.environ.get('api_key')

def getDetails():
    
    # Cria a lista unicas de filmes
    movies_list = []
    movies = tmdb.Movies()
    person = []

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
    
    return movie_list
                        
if __name__ == '__main__':
    movies = Movie()
    movies_list = getDetails()
    movies.getCrewFormListMovie(movies_list)
