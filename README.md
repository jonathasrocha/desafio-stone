# Desafio Stone pagamento #
Durante a etapa de seleção para a empresa Stone pagamentos foi proposto um desafio para ler o conteudo do site [the movie database](https://www.themoviedb.org/), uma base de dados grátis e de código aberto sobre Filmes e Séries de TV. Criado por Travis Bell em 2008, e responder algumas perguntas acerca dos dados.
Logo, apresento uma soluçao de etl utilizando Apache airflow, Amazon S3 e Amazon redshift. 

## Data-warehouse ##


Durante a modelagem de dados pensei numa estrutura dimensional com quatro dimensões d\_date, d\_movie, d\_person, d\_genre e cinco tabela fato f\_status, f\_production\_countries, f\_classification, f\_cost, f\_crew no total de nove tabelas, seu diagrama relacional ficou assim:


![](https://workolistexample.s3.amazonaws.com/data-set/schema.png)




Em seguida é apresentado os aspectos de arquitetura.


## Arquitetura proposta ##


Na arquitetura foi utilizado o apache-airflow, Amazon S3 e Amazon Redshift. Foi escolhido esse conjunto primeiramente pelo apache-airflow por usar python para fazer etl uma linguaguem aplamente usada para analise de dados e ciência de dados, o conjunto amazon S3 e amazon Redshift por possuir a arquitetura de processamento paralelo em massa para carregar os dados do S3 para redshift, essa função pode ser amplamente explorada, fatiando arquivos grandes para tamanhos menores, em seguida é apresentado os detalhes de construção.


![](https://workolistexample.s3.amazonaws.com/data-set/fluxo_s3_redshift.png)

Basicamente a elaboração da estrutura segue os seguintes passos:


1. Criar o bucket S3 e o cluster Redshift.
2. Criar o esquema do banco no redshift.
3. Transformar os arquivo, carregar no S3 e copiar para o Redshift.


### Criar bucket S3 E Cluster Redshift ###


Foi configurado as credenciais AMI e instalado a amazon cli para poder interagir os servicos da amazon pela linha de comando, foi executado o seguinte script para criar o bucket S3 chamado stones11323 e o cluster redshift com dois nós dc2.large.

    
    	#!/bin/bash -e
	#aws s3 mb s3://stones11323

	#create cluster with paramets passed from enviroment variable
	aws redshift create-cluster --cluster-identifier $cluster_name --node-type ds2.large --number-of-nodes 2 --db-name $redshift_dbname --master-username $redshift_username --master-user-password $redshift_password


	#print mensage to screen
	echo "Waiting for redshift endpoint"

	#Waiting for the cluster to stay available
	aws redshift wait cluster-available --cluster-indentifier $cluster_name

	#get the endpoint to connect and create datawarehouse
	endpoint=$(aws redshift describe-clusters --cluster-identifier $cluster_name --query "Clusters[*].Endpoint.Address" --output text)
	port=$(aws redshift describe-clusters --cluster-identifier $cluster_name --query "Clusters[*].Endpoint.Port" --output text)`



### Criar o esquema do banco no redshift. ###


Com o endpoint do banco retornado e armazenado na variável de ambiente endpoint, é feito a conexão com o banco utilizando o psql:

	#connect to just cluster created 
	psql --host=$endpoint --port=$port --username=$redshift_username --dbname=$redshift_dbname -f model.sql
	#write at database the script within at model.sql
	psql -f model.sql	


Após a conexão é feita a importação do script abaixo chamado *model.sql.*



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
		person_id BIGINT PRIMARY KEY UNIQUE,
		name VARCHAR(35),
		profile_path VARCHAR(150),
	gender VARCHAR(2)
	);

	CREATE TABLE IF NOT EXISTS 
	public.d_movie(
		movie_id BIGINT PRIMARY KEY UNIQUE, 
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
		genre_id BIGINT PRIMARY KEY UNIQUE,
		name VARCHAR(200)
	);

	CREATE TABLE IF NOT EXISTS 
	public.f_crew(
		movie_id BIGINT REFERENCES d_movie(movie_id),
		person_id BIGINT  REFERENCES d_person(person_id),
		job VARCHAR(200),
		departament VARCHAR(200),
		UNIQUE(movie_id, person_id, job)
	)
	compound sortkey(movie_id, person_id, job);

	CREATE TABLE IF NOT EXISTS
	public.f_cost(
		movie_id BIGINT REFERENCES d_movie(movie_id),
		budget DECIMAL(18,2),
		revenue DECIMAL(18,2),
	release_date DATE,
	company_name VARCHAR(200),
	UNIQUE(movie_id, company_name, release_date)
	)
	compound sortkey(movie_id, company_name, release_date);

	CREATE TABLE IF NOT EXISTS
	public.f_classification(
		movie_id BIGINT REFERENCES d_movie(movie_id),
		genre_id BIGINT REFERENCES d_genre(genre_id),
	release_date DATE,
	UNIQUE(movie_id, genre_id)
	)
	compound sortkey(movie_id, genre_id);

	CREATE TABLE IF NOT EXISTS
	public.f_production_countries(
	movie_id BIGINT REFERENCES d_movie(movie_id),
	release_date TIMESTAMP,
	iso_3166_1 VARCHAR(2),
	name VARCHAR(100),
	UNIQUE(movie_id, release_date, iso_3166_1)
	)
	compound sortkey(movie_id, release_date, iso_3166_1);

	CREATE TABLE IF NOT EXISTS
	public.f_status(
		movie_id BIGINT REFERENCES d_movie(movie_id),
		date_status DATE DEFAULT GETDATE(),
		status VARCHAR(100),
		UNIQUE(movie_id, date_status)
	)
	compound sortkey(movie_id, date_status);

	CREATE TABLE d_movie_stage(LIKE d_movie);
	CREATE TABLE d_genre_stage(LIKE d_genre);
	CREATE TABLE d_date_stage(LIKE d_date);
	CREATE TABLE d_person_stage(LIKE d_person);
	CREATE TABLE f_cost_stage(LIKE f_cost);
	CREATE TABLE f_production_countries_stage(LIKE f_production_countries);
	CREATE TABLE f_status_stage(LIKE f_status);
	CREATE TABLE f_classification_stage(LIKE f_classification);
	CREATE TABLE f_crew_stage(LIKE f_crew);

### Processo de etl ###


Nessa fase foi utilizado o airflow executando 22 passos abaixo. primeiro é lido os dados do filmes em cartaz e em lancamento com o passo read\_movies e os generos paralelamente, em seguida, também é executado em paralelo os detalhes do filme e as comissões técnica, apos a leitura da comissao tecnica é lido todos filmes produzidos pelos diretores, após todas essas etapas terem sidos construidas, é feito upload dos arquivos no s3 no passo upload\_to\_S3. nos penultimos passos com oito passos em paralelo é copiado os dados do s3 para o redshift em uma tabela temporária nos passos com inicial load\_s3 e finalizando com o passo merge\_s3 que atualiza a os dados no s3 incrementalmente. logo a adiante é apresentado as respostas para as perguntas


![](https://workolistexample.s3.amazonaws.com/data-set/airflow_step.png)


### Respostas  ###

[Respostas](http://bit.ly/2P4HFkP)
