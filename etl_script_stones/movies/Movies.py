from requests.exceptions import HTTPError
import tmdbsimple as tmdb
import os
import csv
import json

tmdb.API_KEY = os.environ.get('api_key')
movies = tmdb.Movies()

class Movies():

    def getDetailFromListMovie(self, movies_list):
        """
        Esta funcao recebe uma lista de filmes obtem seus detalhes e exporta os arquivo csv correspondente a cada fato
        """
        print("Total movies %d"%len(movies_list))
        i = 0
        
    
        production_path = 'data/production_countries.csv'
        classification_path = 'data/classification.csv'
        cost_path = 'data/cost.csv'
        

        with open(cost_path, 'a') as cost, open(production_path, 'a') as prod_countries, open(classification_path, 'a') as classi:

            #Declara os escritores de arquivos e seus cabeçalhos

            cost_writer = csv.DictWriter(cost, fieldnames =['movie_id', 'budget', 'revenue', 'release_date','companie_id','companie_name'])
            p_countries_writer = csv.DictWriter( prod_countries, fieldnames=['movie_id', 'release_date', 'iso_3166_1', 'name'])
            classification_writer = csv.DictWriter( classi, fieldnames=['movie_id', 'genre_id'])

            cost_writer.writeheader()
            p_countries_writer.writeheader()
            classification_writer.writeheader()

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
                i+=1

