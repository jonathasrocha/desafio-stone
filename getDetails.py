from requests.exceptions import HTTPError
import tmdbsimple as tmdb
import os
import csv
import json

tmdb.API_KEY = os.environ.get('api_key')

def getDetails():

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

    print("Total movies %d"%len(movies_list))
    i = 0

    with  open('data/cost.csv', 'w') as cost, open('data/production_countries.csv', 'w') as prod_countries, open('data/classification.csv', 'w') as classi:
        # Atribui os filmas a lista
        for movie_id in movies_list:
            movies.id = movie_id 
            movie = ""
            
            cost_writer = csv.DictWriter(cost, fieldnames =['revenue', 'budget', 'movie_id', 'release_date', 'director_id'])
            pc_writer = csv.DictWriter( prod_countries, fieldnames=['movie_id', 'release_date', 'iso_3166_1'])
            c_writer = csv.DictWriter( classi, fieldnames=['movie_id', 'genre_id'])

            cost_writer.writeheader()
            pc_writer.writeheader()
            c_writer.writeheader()
            
            print("Total processed {} {}%".format(i, 100* (1.0*i/len(movies_list))))
            try:
                movie = movies.info()
            except HTTPError:  
                continue
            
            if(movie):  
                for production_countrie in movie.production_countries:
                    pc_writer.write({'movie_id', movie.id, 
                                    'release_date', movie.release_date,
                                    'iso_3166_1': production_countrie.iso_3166_1})
                                
                for production_companie in movie.production_companies:
                    cost_writer.writer({'movie_id': movie.id, 
                                        'budget': movie.budget/float(len(movie.production_companies)), 
                                        'revenue': movie.revenue/float(len(movie.production_companies)), 
                                        'release_date': movie.release_date, 
                                        'companie_id': production_companie.id }

                for genre in movie.genres
                    c_writer.write({ 'movie_id': movie.id, 'genre_id': genre.id})
            i+=1

if __name__ == '__main__':
    getDetails()
