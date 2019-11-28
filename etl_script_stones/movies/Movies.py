from requests.exceptions import HTTPError
import tmdbsimple as tmdb
import os
import csv
import json
from datetime import datetime

tmdb.API_KEY = os.environ.get('api_key')
movies = tmdb.Movies()

class Movies():

    def getDetailFromListMovie(self, movies_list):
        """
        Esta funcao recebe uma lista de filmes obtem seus detalhes e exporta os arquivo csv correspondente a cada fato
        """
        print("Total movies %d"%len(movies_list))
        i = 0
        
        timestamp = datetime.now().strftime("%H%M%S")
    
        production_path = 'data/production_countries_{}.csv'.format(timestamp)
        classification_path = 'data/classification_{}.csv'.format(timestamp)
        cost_path = 'data/cost_{}.csv'.format(timestamp)
        movies_path = 'data/movies_{}'.format(timestamp)

        with open(cost_path, 'a') as cost, open(production_path, 'a') as prod_countries, open(classification_path, 'a') as classi, open(movies_path, 'a') as movie_file:

            #Declara os escritores de arquivos e seus cabeçalhos

            cost_writer = csv.DictWriter(cost, fieldnames =['movie_id', 'budget', 'revenue', 'release_date','companie_id','companie_name'])
            p_countries_writer = csv.DictWriter( prod_countries, fieldnames=['movie_id', 'release_date', 'iso_3166_1', 'name'])
            classification_writer = csv.DictWriter( classi, fieldnames=['movie_id', 'genre_id'])
            movie_writer = csv.DictWriter(movie_file, fieldnames=['movie_id', 'title', 'original_language', 'populary', 'poster_path', 'adult', 'vote_average'])
            
            cost_writer.writeheader()
            p_countries_writer.writeheader()
            classification_writer.writeheader()
            movie_writer.writeheader()

            for movie_id in movies_list:

                movies.id = movie_id
                movie = ""


                print("Total processed movie {} {}%".format(i, 100* (1.0*i/len(movies_list))))
                try:
                    movie = movies.info()
                except HTTPError:
                    continue

                if(movie):
                    for production_countrie in movies.production_countries:
                        p_countries_writer.writerow({'movie_id': movies.id,
                                            'release_date': movies.release_date,
                                            'iso_3166_1': production_countrie.get('iso_3166_1'),
                                            'name': production_countrie.get('name')})

                    for production_companie in movies.production_companies:
                        cost_writer.writerow({'movie_id': movies.id,
                                            'budget': movies.budget/float(len(movies.production_companies)),
                                            'revenue': movies.revenue/float(len(movies.production_companies)),
                                            'release_date': movies.release_date,
                                            'companie_id': production_companie.get('id'),
                                            'companie_name': production_companie.get('name')})
                    for genre in movies.genres:
                        classification_writer.writerow({ 'movie_id': movies.id, 'genre_id': genre.get('id')})
                
                    movie_writer.writerow({'movie_id': movies.id, 
                                            'title': movies.title, 
                                            'original_language': movies.original_language,
                                            'populary': movies.popularity,
                                            'poster_path': movies.poster_path,
                                            'adult': movies.adult,
                                            'vote_average': movies.vote_average})
                i+=1
        
    def getCrewFormListMovie(self, movies_list):
        print("Total movies %d"%len(movies_list))
        i = 0
        person = []
        
        
        timestamp = datetime.now().strftime("%H%M%S")
        crew_filename = 'data/crew.csv'.format(timestamp)
        person_filename = 'data/person_{}.csv'.format(timestamp)

        with  open(crew_filename, 'a') as crew_file, open(person_filename, 'a') as person_file:

            crew_writer = csv.DictWriter(crew_file, fieldnames=['movie_id', 'person_id', 'job', 'department'])
            person_writer = csv.DictWriter(person_file, fieldnames=['id', 'name', 'profile_path', 'gender'])

            #crew_writer.writeheader()
            person_writer.writeheader()

            # Atribui os filmas a lista
            for movie_id in movies_list:
                movies.id = movie_id

                print("Total processed {} {}%".format(i, 100* (i/len(movies_list))))
                try:
                    movies.credits()
                except HTTPError:
                    continue
                
                if(movies.crew):

                    for crew in movies.crew:
                        if crew.get('id') not in person:
                            person.append(crew.get('id'))
                            person_writer.writerow({'id': crew.get('id'),
                                                    'name': crew.get('name'),
                                                    'profile_path': crew.get('profile_path'),
                                                    'gender': crew.get('gender')})
                    
                        crew_writer.writerow({'movie_id': movies.id,
                                            'person_id': crew.get('id'),
                                            'job': crew.get('job'),
                                            'department': crew.get('department')})
                i+=1


