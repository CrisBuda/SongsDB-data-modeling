# SongsDB data modeling
Python ETL pipeline for data modeling with PostgresSQL

Dataset from http://millionsongdataset.com/ (used a subset due to size)

This project models user activity data from some song playing app. I use a relational database (PostgreSQL) and an ETL pipeline (Python) to model data and insert records corresponding to different tables based on logfiles (JSON). The logfiles are analyzed to show what songs different users are listening to.

Example of a song JSON:
```JSON
{
  "num_songs": 1,
  "artist_id": "ARD842G1187B997376",
  "artist_latitude": 43.64856,
  "artist_longitude": -79.38533,
  "artist_location": "Toronto, Ontario, Canada",
  "artist_name": "Blue Rodeo",
  "song_id": "SOHUOAP12A8AE488E9",
  "title": "Floating",
  "duration": 491.12771,
  "year": 1987
}
```

Example of a user log JSON:

```JSON
{
  "artist": "Flogging Molly",
  "auth": "Logged In",
  "firstName": "Chloe",
  "gender": "F",
  "itemInSession": 1,
  "lastName": "Cuevas",
  "length": 361.9522,
  "level": "free",
  "location": "San Francisco-Oakland-Hayward, CA",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1540940782796,
  "sessionId": 437,
  "song": "Rebels of the Sacred Heart",
  "status": 200,
  "ts": 1541932063796,
  "userAgent": "Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0",
  "userId": "49"
}
```


#Database schema:

![Schema](https://github.com/CrisBuda/SongsDB-data-modeling/blob/master/db.png)

#How to run:

1. Make sure you have postgreSQL server up and running. Default id: postgres pw: test (change in create_tables.py)
2. ``python main.py``
