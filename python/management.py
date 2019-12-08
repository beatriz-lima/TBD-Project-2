import random
import os

import click
import pandas as pd
from neo4j import GraphDatabase

import NodifyDB
import extractionNOSQL


BANDS_LIST = ["id", "dbpedia", "name", "start", "end"]
ALBUMS_LIST = ["id", "name", "dbpedia"]
ARTISTS_LIST = ["id", "dbpedia", "name"]
GENRES_LIST = ["id", 'name']
YEAR_LIST = ['id', 'name']

BANDS_ARTISTS_LIST = ['isActive','exitCount']

ALBUMGENRE_LIST = ["name"]

def create_driver():
    return GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "test"),
    )


def get_data_dir():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "data",
    )


@click.group()
def cli():
    pass


@click.command(help="Creates nodes and edges in the DB.")
def create():
    data_dir = get_data_dir()
    driver = create_driver()
    KINDS = ['BANDS', 'ALBUMS', 'ARTISTS', 'GENRES', 'YEAR']

    LINKS = [['BANDS','ALBUMS', 'madeAlbum'], ['ALBUMS','GENRES', 'isGenre'],['BANDS','ARTISTS','participatesIn'], ['GENRES', 'GENRES', 'derivativeOf'], ['ALBUMS', 'YEAR', 'releasedIn']]

    with click.progressbar(KINDS, label='Loading nodes') as bar:
        for kind in bar:
            path = os.path.join(data_dir, f"{kind.lower()}_processed.csv")
            constitution = globals().get(f'{kind}_LIST')

            if not os.path.exists(path):
                if kind == "GENRES":
                    getattr(extractionNOSQL, f"process_albums")(data_dir)
                else:
                    getattr(extractionNOSQL, f"process_{kind.lower()}")(data_dir)

            table = pd.read_csv(path)
            buffer = 0
            query = "CREATE "

            for index, row in table.iterrows():
                query += NodifyDB.createNode(
                    kind,
                    f'{kind}_{row.values[0]}',
                    constitution,
                    row.values.tolist()
                ) + ","
                buffer += 1
                if buffer % 1000 == 0:
                    with driver.session() as session:
                        session.run(query[:-1])
                    query = "CREATE "
                    buffer = 0

            if buffer:
                with driver.session() as session:
                    session.run(query[:-1])

    with click.progressbar(LINKS, label='Loading links') as park:
        for link in park:

            path = os.path.join(data_dir, f"{link[0].lower()}_{link[1].lower()}_processed.csv")
            attributes = globals().get(f'{link[0]}_{link[1]}_LIST', None)

            if not os.path.exists(path):
                if link[0] == "GENRES" and link[1] == "GENRES":
                    getattr(extractionNOSQL, "process_genre_derivatives")(data_dir)
                elif link[0] == "ALBUMS" and link[1] == "YEAR":
                    getattr(extractionNOSQL, "process_albums_year")(data_dir)
                elif (link[0] == "BANDS" and link[1] == "ALBUMS") or (link[0] == "ALBUMS" and link[1] == "GENRES"):
                    getattr(extractionNOSQL, f"process_albums")(data_dir)
                else:
                    getattr(extractionNOSQL, f"process_artistparticipation")(data_dir)

            table = pd.read_csv(path)

            with driver.session() as session:
                [NodifyDB.createLinkage(session.begin_transaction(),
                    link,
                    f'{int(row.values[0])}',
                    f'{int(row.values[1])}',
                    attributes,
                    row.values[2:].tolist(),
                    True,
                    ) for index, row in table.iterrows()]

    driver.close()


@click.command(help="Generate random data for playlists")
@click.option("--number", type=int, default=25)
def generate_playlists(number):
    driver = create_driver()

    with driver.session() as session:
        num_albums = session.run("MATCH (n:ALBUMS) RETURN count(n)").value()[0]
        num_playlists = session.run("MATCH (p:PLAYLISTS) RETURN count(p)").value()[0]
        album_range = list(range(1, num_albums + 1))

        with click.progressbar(length=number, label='Generating playlists') as bar:
            for _ in range(number):
                albums = random.sample(
                    album_range,
                    random.randrange(10, 50),
                )
                session.run("CREATE (PLAYLISTS_" + str(num_playlists) + ":PLAYLISTS {id: " + str(num_playlists) + "})")
                for album in albums:
                    session.run(f"MATCH (a:ALBUMS), (p:PLAYLISTS) WHERE a.id = {album} AND p.id = {num_playlists} CREATE (a)-[r:isIn]->(p)")

                num_playlists += 1


if __name__ == '__main__':
    cli.add_command(create)
    cli.add_command(generate_playlists)
    cli()