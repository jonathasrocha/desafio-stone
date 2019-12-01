CREATE TABLE if not exists 
public.d_date(
	date_sk BIGINT IDENTITY(0,1) PRIMARY KEY,
	field_date DATE,
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
	half VARCHAR(1)
);

CREATE TABLE IF NOT EXISTS 
public.d_person(
	person_id BIGINT PRIMARY KEY,
	name VARCHAR(35),
	profile_path VARCHAR(150),
	gender VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS 
public.d_movie(
	movie_id BIGINT PRIMARY KEY, 
	title VARCHAR(150),
	original_language VARCHAR(2),
	popularity DOUBLE PRECISION,
	poster_path VARCHAR(150),
	adult BOOLEAN,
	status VARCHAR(50),
	vote_average DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS 
public.d_genre(
	genre_id BIGINT PRIMARY KEY,
	name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS 
public.f_crew(
	movie_sk BIGINT REFERENCES d_movie(movie_id),
	person_sk BIGINT  REFERENCES d_person(person_id),
	job VARCHAR(200),
	departament VARCHAR(200)
)
compound sortkey(movie_sk, person_sk, job);

CREATE TABLE IF NOT EXISTS
public.f_cost(
	movie_sk BIGINT REFERENCES d_movie(movie_id),
	budget DECIMAL(18,2),
	revenue DECIMAL(18,2),
	release_date DATE,
	company_name VARCHAR(200)
)
compound sortkey(movie_sk, company_name, release_date);

CREATE TABLE IF NOT EXISTS
public.f_classification(
	movie_sk BIGINT REFERENCES d_movie(movie_id),
	genre_sk BIGINT REFERENCES d_genre(genre_id),
	release_date DATE
)
compound sortkey(movie_sk, genre_sk);

CREATE TABLE IF NOT EXISTS
public.f_production_countries(
	movie_sk BIGINT REFERENCES d_movie(movie_id),
	release_date TIMESTAMP,
	iso_3166_1 VARCHAR(2),
	name VARCHAR(100)
)
compound sortkey(movie_sk, release_date, iso_3166_1);

CREATE TABLE IF NOT EXISTS
public.f_status(
	movie_sk BIGINT REFERENCES d_movie(movie_id),
	date_status DATE DEFAULT GETDATE(),
	status VARCHAR(100)
)
compound sortkey(movie_sk, date_status);

CREATE TABLE d_movie_stage(LIKE d_movie);
CREATE TABLE d_genre_stage(LIKE d_genre);
CREATE TABLE d_date_stage(LIKE d_date);
CREATE TABLE d_person_stage(LIKE d_person);
CREATE TABLE f_cost_stage(LIKE f_cost);
CREATE TABLE f_production_countries_stage(LIKE f_production_countries);
CREATE TABLE f_status_stage(LIKE f_status);
CREATE TABLE f_classification_stage(LIKE f_classification);
CREATE TABLE f_crew_stage(LIKE f_crew);

