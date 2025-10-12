# Data Modeling in Postgres for Music Startup, Sparkify 

## Overview: Database, business context, analytical goals

The following Postgres database has been designed to easily access Sparkify's new music streaming app data to gain further insight into their customers habits and listening patterns. Data includes users, songs, and user activity. 


## How to: Running Python scripts, Jupyter

Run the following scripts in the Juptyer notebook by selecting the interested cell contatining your query and clicking 'Run' in the panel above. Or by calling the functions individually via the 'etl.py' file.


## Files:

* 'data' - The current data that we ate working with.
* 'etl.pynb' - The notebook analyzing the data as well as process of understanding the code.
* 'test.pynb' - Code tests ensuring functionality.
* 'create_tables.py' - Functions for creating our tables.
* 'etl.py' - Python functions to run our code independent of the notebook.
* 'sql_queries.py' - SQL queries for creating tables and inserting data into them.
* 'README.md' - Description of project.


## Design: Database schema design, ETL pipeline, and justification

The database schema has the following dimension tables: 'users', 'songs, 'artists;, and 'time', as well as one fact table called 'songplays'. The ETL pipeline pulls up to date data provided by Sparkify and includes a combination of 'song_data' and 'log_data' files combined to tie users, and time spent listening to the songs data for user activity insights. These table have been designed to optimize queries on song play analysis of Sparkify's users. 
