The goal of this project is to showcase the different capabilities of a NoSQL database vs Relational database. For that we designed a graph database on Neo4J and implemented two complex operations.

Node type:
- Artist
- Band
- Album
- Genre
- Playlist

Relations:
- Participation (Artist-Band) (With possible exitCount, have another relation)
- Produces (Band-Album)
- belongsTo (Album-Genre)
- hasAlbum (Playlist-Album)
- Feature (Artist-band-album)

Queries:
Filtrar por generos
Kevin Bacon Score
Band -> Albums -> Genre -> Playlists

1. Similarity between Bands given two patterns Band -> Albums -> Genre -> Playlists.

2. Playlist suggestions (?)