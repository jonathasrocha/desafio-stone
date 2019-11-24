import tmdbsimple as tmdb
import csv
import os

#Defino a chave copiando da viariavel de ambiente
tmdb.API_KEY = os.environ.get('api_key')

def getNowplayingMovies():
    
    movies = tmdb.Movies()
    movies_now_playing = movies.now_playing()
    pages = movies_now_playing.get('total_pages')
    
    fieldnames = movies_now_playing.get('results')[0].keys()

    with open('data/movies_nowplaying.csv', 'w', newline='') as movies_csv: 
        writer = csv.DictWriter(movies_csv, fieldnames = fieldnames)
        writer.writeheader()
        for page in range(1, pages +1):
            movies_now_playing = movies.now_playing(page = page)
            for movie in movies_now_playing.get('results'):
                writer.writerow(movie)

def getUpcomingMovies():
    
    movies = tmdb.Movies()
    movies_now_playing = movies.upcoming()

    pages = movies_now_playing.get('total_pages')
    
    fieldnames = movies_now_playing.get('results')[0].keys()

    with open('data/movies_upcoming.csv', 'w', newline='') as movies_csv: 
        writer = csv.DictWriter(movies_csv, fieldnames = fieldnames)
        writer.writeheader()
        for page in range(1, pages +1):
            movies_now_playing = movies.now_playing(page = page)
            for movie in movies_now_playing.get('results'):
                writer.writerow(movie)

if __name__ == '__main__':
    getNowplayingMovies()
    getUpcomingMovies()