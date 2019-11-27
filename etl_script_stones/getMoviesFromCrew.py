import os
import csv
import tmdbsimple as tmdb
from requests.exceptions import HTTPError
from collections import namedtuple
from movies.Movies import Movies 

tmdb.API_KEY = os.environ.get('api_key')

def getMoviesFromJob(job="Director"):

    # Cria a lista unicas de filmes
    movies_list = []
    person = tmdb.People()
    person_director = []

    # Le a equipe tecnica, separa por diretor e busca seus filmes 
    with  open('data/crew.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader, None)
        for crew in reader:
            if crew[2] == job and crew[0] not in movies_list:
                movies_list.append(crew[0])
    return movies_list

if __name__ == '__main__':
    movies = Movies()
    movies_list = getMoviesFromJob()
    movies.getDetailFromListMovie(movies_list)
