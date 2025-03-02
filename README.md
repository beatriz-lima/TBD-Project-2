The goal of this project is to showcase the different capabilities of a NoSQL database vs Relational database. For that we designed a graph database on Neo4J and implemented two complex operations.

Node type   || Associated Attributes:
- Artist    || name ; dbpedia
- Band      || name ; dbpedia ; yearStart ; yearEnd
- Album     || name ; dbpedia ; sold ; duration ; abstract
- Genre     || name
- Playlist  || uniqueID ; name
- user      || uniqueID ; name (Uma vez que faz aqui faz mais sentido alguem ser dono de uma playlist. Gerar uses falsos.)

Relations || Attributes:
- Participation (Artist-Band) (With possible exitCount, have another relation) || nTimes ; activeIn
- Produces (Band-Album)
- ofGenre (Album-Genre)
- hasAlbum (Playlist-Album)
- Feature (Artist-band-album)

Queries:
Filtrar por generos
Kevin Bacon Score
Band -> Albums -> Genre -> Playlists

1. Similarity between Bands given two patterns Band -> Albums -> Genre -> Playlists.

2. Playlist suggestions (?)

# Running Neo4j using Docker

## Step 1: Pulling image

Execute the following command: `bash docker/neo4j/pull-image.sh` in a terminal.

This step only needs to be done once, after doing it, you can skip this step.

## Step 2: Running Neo4j

To run the Neo4j server, run the following command in a terminal: `bash docker/neo4j/start-container.sh --rm`.

For more information of extra options, run `bash docker/neo4j/start-container.sh --help`.

To stop the server, press CTRL+C with the terminal in focus.

Neo4j comes by default with a browser, which can be accessed on localhost:7474, the default login is username: neo4j, password: neo4j.
