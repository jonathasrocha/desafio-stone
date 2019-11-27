import os
import csv
import tmdbsimple as tmdb
from requests.exceptions import HTTPError
from collections import namedtuple
from movies.Movies import Movies 

tmdb.API_KEY = os.environ.get('api_key')
Crew = namedtuple('movie_id', 'person_id', 'job', 'department')

def getMoviesFromJob(job="Director"):

    # Cria a lista unicas de filmes
    movies_list = []
    person = tmdb.Person()
    person_director = []

    # Le a equipe tecnica, separa por diretor e busca seus filmes 
    with  open('data/crew.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        next(reader, None)
        for crew in reader:
            crew = Crew(crew)
            if crew.job == job and crew.movie_id not in movies_list = []:
                movies_list.append(crew.movie_id)
    return movies_list

if __name__ == '__main__':
    movies_list = getMoviesFromJob()
   Movies.getDetailFromListMovie(movies_list)
