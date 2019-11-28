CREATE TABLE if not exists 
public.d_date(
	date_sk BIGINT IDENTITY(0,1) PRIMARY KEY,
	field_date TIMESTAMP,
	year SMALLINT,
	month SMALLINT,
	day_of_year SMALLINT,
	day_of_mouth SMALLINT,
	day_of_week SMALLINT,
	week_of_year SMALLINT,
	day_of_week_desc VARCHAR(30),	
	day_of_week_desc_short VARCHAR(30),
	month_desc VARCHAR(30),
	month_desc_short VARCHAR(3),
	quarter VARCHAR(1),
	half VARCHAR(1),
);

CREATE TABLE IF NOT EXISTS 
public.d_person(
	person_id DOUBLE PRECISION PRIMARY KEY,
	name VARCHAR(35),
	profile_path VARCHAR(150),
);

CREATE TABLE IF NOT EXISTS 
public.d_movie(
	movie_id DOUBLE PRECISION PRIMARY KEY, 
	title VARCHAR(150),
	original_language VARCHAR(2),
	populary DOUBLE PRECISION,
	poster_path VARCHAR(150),
	adult BOOLEAN,
	status VARCHAR(50),
	vote_average DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS 
public.d_genre(
	genre_id DOUBLE PRECISION PRIMARY KEY,
	name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS 
public.f_crew(
	movie_id DOUBLE PRECISION REFERENCES d_movie(movie_id),
	person_id DOUBLE PRECISION REFERENCES d_person(person_id),
	job VARCHAR(200),
	departament VARCHAR(200)
)
compound sortkey(movie_id, person_id, job);

CREATE TABLE IF NOT EXISTS
public.f_cost(
	cost BIGINT IDENTITY(0,1) PRIMARY KEY,
	movie_id DOUBLE PRECISION REFERENCES d_movies(movie_id),
	company_name VARCHAR(200),
	budget DOUBLE PRECISION,
	revenue DOUBLE PRECISION,
	release_date DATE
)
compound sortkey(movied, company_name, release_date);

CREATE TABLE IF NOT EXISTS
public.f_classification(
	movie_id DOUBLE PRECISION REFERENCES d_movies(movie_id),
	genre_id DOUBLE PRECISION REFERENCES d_genre(genre_id),
	release_date DATE
)
compound sortkey(movie_id, genre_id);

CREATE TABLE IF NOT EXISTS
public.f_production_contries(
	movie_id DOUBLE PRECISION REFERENCES d_movies(movie_id),
	release_date TIMESTAMP,
	iso_3166_1 VARCHAR(2),
	name VARCHAR(100)
)
compound sortkey(movie_id, release_date, iso_3166_1);

CREATE TABLE IF NOT EXISTS
public.f_status(
	movie_id DOUBLE PRECISION REFERENCES d_movies(movie_id),
	date_status DATE DEFAULT GETDATE(),
	status VARCHAR(100)
)
compound sortkey(movie_id, date_status, status)
