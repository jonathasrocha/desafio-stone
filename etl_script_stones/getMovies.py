import tmdbsimple as tmdb
import csv
import os

#Defino a chave copiando da viariavel de ambiente
tmdb.API_KEY = os.environ.get('api_key')

def getNowplayingMovies():
    
    movies = tmdb.Movies()
    movies_now_playing = movies.now_playing(region='US')
    pages = movies_now_playing.get('total_pages')
    
    fieldnames = movies_now_playing.get('results')[0]
    fieldnames['status'] = ''
    fieldnames = fieldnames.keys()

    with open('data/movies_nowplaying.csv', 'w', newline='') as movies_csv: 
        writer = csv.DictWriter(movies_csv, fieldnames = fieldnames)
        writer.writeheader()
        for page in range(1, pages +1):
            movies_now_playing = movies.now_playing(page = page, region='US')
            for movie in movies_now_playing.get('results'):
                movie['status'] = 'now playing'
                writer.writerow(movie)

def getUpcomingMovies():
    
    movies = tmdb.Movies()
    movies_now_playing = movies.upcoming(region='US')
    pages = movies_now_playing.get('total_pages')
    
    fieldnames = movies_now_playing.get('results')[0]
    fieldnames['status'] = ''
    fieldnames = fieldnames.keys()

    with open('data/movies_upcoming.csv', 'w', newline='') as movies_csv: 
        writer = csv.DictWriter(movies_csv, fieldnames = fieldnames)
        writer.writeheader()
        for page in range(1, pages +1):
            movies_now_playing = movies.now_playing(page = page, region='US')
            for movie in movies_now_playing.get('results'):
                movie['status'] = 'up coming'
                writer.writerow(movie)

if __name__ == '__main__':
    getNowplayingMovies()
    getUpcomingMovies()
