import os

import click
import pandas as pd
from neo4j import GraphDatabase

import NodifyDB
import extractionNOSQL


BANDS_LIST = ["dbpedia", "name", "start", "end"]
ALBUMS_LIST = ["name", "dbpedia", "sold", "duration", "abstract"]
ARTIST_LIST = ["name", "dbpedia"]
ALBUMGENRE_LIST = ["name"]
ARTISTPARTICIPATION_LIST = ["nTimes", "activeIn"]


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
    KINDS = ['BANDS', 'ALBUMS', 'ARTISTS', 'ALBUMGENRE', 'ARTISTPARTICIPATION']

    with click.progressbar(KINDS, label='Loading nodes') as bar:
        for kind in bar:
            path = os.path.join(data_dir, f"{kind.lower()}_processed.csv")
            constitution = globals().get(f'{kind}_LIST')

            if not os.path.exists(path):
                getattr(extractionNOSQL, f"process_{kind.lower()}")(data_dir, constitution)

            table = pd.read_csv(path)

            create = "CREATE " + ','.join([
                NodifyDB.createNode(
                    kind,
                    f'{kind}_{row.values[0]}',
                    constitution,
                    row.values[1:].tolist()
                ) for index, row in table.iterrows()])

            with driver.session() as session:
                tx = session.begin_transaction()
                tx.run(create)
                tx.commit()

    driver.close()


if __name__ == '__main__':
    cli.add_command(create)
    cli()