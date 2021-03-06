# Data Modeling with Apache Cassandra

<p align="center">
  <img src="sparkify.png" alt="Sublime's custom image"/>
</p>
 

## **Overview**
In this project, we create data modeling with Apacahe Cassandra and build ETL pipeline using python.
**Study Case** : A startup in indonesia wants to analyze the data they have been collecting on songs and user activity on their new music streaming app.
Currently, this startup collecting data in json format and the analytics team is particularly interested in understanding what songs user are listening to.

 
## **Log Dataset**
Logs dataset is generated by [Event Simulator](https://github.com/Interana/eventsim)

Sample Record :
```json
{"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M", "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free", "location": "San Francisco-Oakland-Hayward, CA", "method": "GET","page": "Home", "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200, "ts": 1541105830796, "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId": "39"}
```

## Schema

#### Fact Table 
**songplays** - records in log data associated with song plays i.e. records with page `NextSong`

```
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

#### Dimension Tables
**user_session**  - users session in the app
```
session_id,user_id,artist, firstname, iteminsession, lastname
```
** user_songs**  - user play songs  
```
song, user_id, firstname, lastname
```
**session_item**  - item in session
```
session_id,iteminsession, artist, length, song
```
 

## Project Files

```sql_queries.py``` -> contains sql queries for dropping and  creating fact and dimension tables. Also, contains insertion query template.

```create_tables.py``` -> contains code for setting up database. Running this file creates **sparkify** and also creates the fact and dimension tables.

```modeling-data.ipynb``` -> a jupyter notebook for testing. 

```etl.py``` -> read and process file in event_data directory

```lib.py``` -> import library that used

```event_datefile_new.csv``` -> output etl process

 
## Environment 
Python 3.6 or above

Apache Cassandra  

cassandra - Cassandra database adapter for Python


## How to run

Run the drive program ```main.py``` as below.
```
python main.py
``` 

The ```create_tables.py``` and ```etl.py``` file can also be run independently as below:
```
python create_tables.py 
python etl.py 
```


 #### Reference: 
[Cassandra](https://github.com/datastax/python-driver)

[Cassandra Documentation](https://cassandra.apache.org/)

