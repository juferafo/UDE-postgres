# UDE-postgres

The goal of this project is to generate a PostgreSQL database that hosts structured data related to song and user activity with the purpose of having a playground where we can obtain quick insigts via SQL queries.

## Song and Log datasets

The data used in this project is contained in two datasets: the Song dataset and the Log dataset. 

### Song dataset

The Song dataset is a subset of the [Million Song Dataset](http://millionsongdataset.com/). The data is organized in with the following file structure 

```
song_data/
└── A
    ├── A
    │   ├── A
    │   ├── B
    │   └── C
    └── B
        ├── A
        ├── B
        └── C
```

where the parameters of the song (song_id, title, duration, etc...) and the information of its artist (artist id, artist name, artist location, etc...) is included in a JSON file. As an example, below you can find the data corresponding to the song `./song_data/A/A/A/TRAAAAW128F429D538.json`

```
{
   "num_songs":1,
   "artist_id":"ARD7TVE1187B99BFB1",
   "artist_latitude":null,
   "artist_longitude":null,
   "artist_location":"California - LA",
   "artist_name":"Casual",
   "song_id":"SOMZWCG12A8C13C480",
   "title":"I Didn't Mean To",
   "duration":218.93179,
   "year":0
}
```

### Log dataset


## Database description

The database 

### Fact Table

1. songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

1. users table
user_id, first_name, last_name, gender, level

2. songs - songs in music database
song_id, title, artist_id, year, duration

3. artists - artists in music database
artist_id, name, location, latitude, longitude

4. time - timestamps of records in songplays broken down into specific units 
start_time, hour, day, week, month, year, weekday

## Data ingestion


## Example queries


## Requirements

In order to run the

