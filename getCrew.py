import os
import csv
import tmdbsimple as tmdb
from requests.exceptions import HTTPError

tmdb.API_KEY = os.environ.get('api_key')

def getCrew():
    
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
     
    with  open('data/crew.csv', 'w') as file:
    
        writer = csv.DictWriter(file, fieldnames=['credit_id', 'department', 'gender', 'id', 'job', 'name', 'profile_path'])
        
        writer.writeheader()
        
        # Atribui os filmas a lista
        for movie_id in movies_list:
            movies.id = movie_id 
            
            print("Total processed {} {}%".format(i, 100* (i/len(movies_list))))
            try:
                movies.credits()
            except HTTPError:
                continue
            if(movies.crew):
               
                for movie in movies.crew:
                    movie = {k: unicode(v).encode("utf-8") for k,v in movie.iteritems()}
                    writer.writerow(movie)
            i+=1
                        
if __name__ == '__main__':
    getCrew()
