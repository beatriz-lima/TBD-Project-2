import os
import NodifyDB

import pandas as pd

#Da-lhe bro - Ruben, 2019

def get_data_dir():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "data",
    )



bands_list = ["dbpedia", "name", "start", "end"]
albums_list = ["name", "dbpedia", "sold", "duration", "abstract"]
artist_list = ["name", "dbpedia"]
albumgenre_list = ["name"]
artistparticipation_list = ["nTimes", "activeIn"]

def load_or_make_df(path, process_function):
    if not os.path.isfile(path):
        process_function(os.path.dirname(path))
    return pd.read_csv(path)


def table_load(kind):
    data_dir = get_data_dir()
    table = pd.read_csv(os.path.join(data_dir, f"{kind}_processed.csv"))
    return(','.join([
        NodifyDB.createNode(
            kind, 
            f'{kind}_{row.values[0]}', 
            globals().get(f'{kind}_list'),
            row.values[1:].tolist()
        ) for index, row in table.iterrows()]))

