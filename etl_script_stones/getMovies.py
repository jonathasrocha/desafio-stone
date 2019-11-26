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

    with open('data/movies_nowplaying.csv', 'w', newline='') as movies_csv, open('data/status_nowplaying.csv', 'w', newline='') as s_now_csv: 
        
        writer = csv.DictWriter(movies_csv, fieldnames = fieldnames)
        writer_status = csv.DictWriter(s_now_csv, fieldnames = ['movie_id', 'status'])
        
        writer_status.writeheader()
        writer.writeheader()
        
        for page in range(1, pages +1):
            movies_now_playing = movies.now_playing(page = page, region='US')
            for movie in movies_now_playing.get('results'):
                writer_status.writerow({'movie_id': movie.get('id'), 'status': 'now playing'})
                writer.writerow(movie)


def getUpcomingMovies():
    
    movies = tmdb.Movies()
    movies_now_playing = movies.upcoming(region='US')
    pages = movies_now_playing.get('total_pages')
    
    fieldnames = movies_now_playing.get('results')[0]
    fieldnames['status'] = ''
    fieldnames = fieldnames.keys()

    with open('data/movies_upcoming.csv', 'w', newline='') as movies_csv, open('data/status_upcoming.csv', 'w', newline='') as s_up_csv: 
        writer = csv.DictWriter(movies_csv, fieldnames = fieldnames)
        
        writer_status = csv.DictWriter(s_up_csv, fieldnames = ['movie_id', 'status'])
        writer.writeheader()
       
        for page in range(1, pages +1):
            movies_now_playing = movies.now_playing(page = page, region='US')
            for movie in movies_now_playing.get('results'):
                writer_status.writerow({'movie_id': movie.get('id'), 'status': 'up coming'})
                writer.writerow(movie)

if __name__ == '__main__':
    getNowplayingMovies()
    getUpcomingMovies()
