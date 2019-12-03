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
    KINDS = ['BANDS', 'ALBUMS', 'ARTISTS', 'GENRES']

    LINKS = [['BANDS','ALBUMS', 'madeAlbum'], ['ALBUMS','GENRES', 'isGenre'],['BANDS','ARTISTS','participatesIn']]
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

    with click.progressbar(LINKS, label='Loading links') as park:
        for link in park:

            path = os.path.join(data_dir, f"{link[0].lower()}_{link[1].lower()}_processed.csv")
            attributes = globals().get(f'{link[0]}_{link[1]}_LIST')

            if not os.path.exists(path):
                if (link[0] == "BANDS" and link[1] == "ALBUMS") or (link[0] == "ALBUMS" and link[1] == "GENRES"):
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


if __name__ == '__main__':
    cli.add_command(create)
    cli()