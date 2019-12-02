import os
import csv
import tmdbsimple as tmdb
from requests.exceptions import HTTPError
from collections import namedtuple
from movies.Movies import Movies 

tmdb.API_KEY = os.environ.get('api_key')
people = tmdb.People()

def getMoviesFromJob(job="Director"):

    # Cria a lista unicas de filmes
    movies_list = []
    person = tmdb.People()
    person_director = []

    # Le a equipe tecnica, separa por diretor e busca seus filmes 
    with  open('data/f_crew_.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader, None)
        for crew in reader:
            if crew[2] == job:
                people.id = crew[1]
                try:
                    credits =people.movie_credits()
                except HTTPError:
                    continue

                for cast in credits.get('cast'):
                    if cast.get('id') not in movies_list:
                        movies_list.append(cast.get('id'))
    return movies_list

if __name__ == '__main__':
    movies = Movies()
    movies_list = getMoviesFromJob()
    movies.getDetailFromListMovie(movies_list)
