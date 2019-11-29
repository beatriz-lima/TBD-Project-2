import os
import re

import pandas as pd
import numpy as np


def load_or_make_df(path, process_function):
    if not os.path.isfile(path):
        process_function(os.path.dirname(path))
    return pd.read_csv(path)


def process_bands(data_dir):
    band_name = pd.read_csv(os.path.join(data_dir, 'band-band_name.csv'))
    band_start_end = pd.read_csv(os.path.join(data_dir, 'band-start_year-end_year.csv'))

    #Merge both DataFrames on their common attribute, the band URL
    bands_df = pd.merge(band_name, band_start_end, on='band', how='outer')

    #Drop any hard duplicate lines
    bands_df = bands_df.drop_duplicates('band')

    #Add a new column named 'id' to the merged DataFrame and posteriorly(?)
    # change the column position to first
    bands_df['id'] = np.arange(1, len(bands_df) + 1)
    #https://stackoverflow.com/questions/13148429/how-to-change-the-order-of-dataframe-columns
    cols = bands_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    bands_df = bands_df[cols]
    dateFix = lambda x: x if (isinstance(x, float) or x.isdigit()) else ""

    bands_df["start"] = bands_df.start.apply(dateFix)
    bands_df["end"] = bands_df.end.apply(dateFix)

    #Save the result DataFrame into a new csv file
    bands_df.to_csv(os.path.join(data_dir, 'bands_processed.csv'), index=False)


def process_albums(data_dir):
    #Condição que verifica se já existe a tabela bands_df.
    #Se não existir, criar
    bands_df = load_or_make_df(
        os.path.join(data_dir, 'bands_processed.csv'),
        process_bands,
    )

    #Get the required csv files to DataFrame
    band_album = pd.read_csv(os.path.join(data_dir, 'band-album_data.csv'))
    band_album_genre = pd.read_csv(os.path.join(data_dir, 'band-album_data_genre.csv'))

    albums_df = pd.merge(band_album, bands_df, on='band', how='outer')

    albums_df_all = pd.merge(albums_df, band_album_genre, on=['band', 'release_date', 'duration', 'album_name'])

    #https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop_duplicates.html
    albums_df = albums_df_all.drop_duplicates(subset=['album','duration', 'release_date'])

    #Select which columns to save and change the column names
    albums_df = albums_df[['album','id','album_name','release_date','duration','sold_x','abstract_x']]
    albums_df.columns = ['URI','bandID','name','release', 'duration', 'sold', 'abstract'] #Está repetido e desnecessário

    #Get arrays of sales, launchDate, durations
    albums_launchDate = albums_df.groupby(['URI']).release.unique().apply(lambda x: x.tolist()).reset_index()
    albums_sold = albums_df.groupby(['URI']).sold.unique().apply(lambda x: x.tolist()).reset_index()
    albums_duration = albums_df.groupby(['URI']).duration.unique().apply(lambda x: x.tolist()).reset_index()

    #Merge and choose columns
    albums_df_temp = pd.merge(pd.merge(albums_launchDate,albums_sold),albums_duration)
    albums_df = pd.merge(albums_df.drop_duplicates('URI'), albums_df_temp, suffixes=['_noArray','_array'], on='URI')
    albums_df = albums_df[['URI','bandID','name','release_array', 'duration_array', 'sold_array', 'abstract']]

    albums_df['id'] = np.arange(1, len(albums_df) + 1)
    cols = albums_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    albums_df = albums_df[cols]

    def fix_sales(x):
        if isinstance(x, int):
            return x
        if isinstance(x, str) and "rowspan" in x:
            return int(re.findall(r"[0-9,0-9]+", x)[-1].replace(",", ""))
        else:
            return x

    _fix_sales = lambda x: list(map(fix_sales, x))
    albums_df["sold_array"] = albums_df["sold_array"].apply(_fix_sales)


    albums_df.columns = ['dbpedia','bandID','name','release', 'duration', 'sold', 'abstract']
    albums_df = albums_df[["name", "dbpedia", "sold", "duration", "abstract"]]


    albums_df.to_csv(os.path.join(data_dir, 'albums_processedNOSQL.csv'), index=False)


def process_albumgenre(data_dir):
    #Get the required csv file to DataFrame
    band_album_genre = pd.read_csv(os.path.join(data_dir, 'band-album_data_genre.csv'))
    band_album = pd.read_csv(os.path.join(data_dir, 'band-album_data.csv'))

    bands_df = load_or_make_df(
        os.path.join(data_dir, 'bands_processed.csv'),
        process_bands,
    )
    albums_df = load_or_make_df(
        os.path.join(data_dir, 'albums_processed.csv'),
        process_albums,
    )

    albums_df = pd.merge(band_album, bands_df, on='band', how='outer')
    albums_df_all = pd.merge(albums_df, band_album_genre, on=['band', 'release_date', 'duration', 'album_name'])

    albums_df = pd.read_csv(os.path.join(data_dir, 'albums_processed.csv'))
    albums_df_genre = albums_df_all[['id','album', 'genre', 'album_name']]
    albums_df_genre.columns = ['bandID', 'URI', 'genre', 'name']
    albums_genre = pd.merge(albums_df_genre, albums_df, on=['URI','bandID', 'name'])

    albums_genre = albums_genre.drop_duplicates()
    albums_genre = albums_genre[['id','bandID','genre']]
    albums_genre.columns = ['albumID','bandID','genre']

    fixGenre = lambda row: row.split("/")[-1].replace("_", " ")

    albums_genre['genre'] = albums_genre.genre.apply(fixGenre)

    albums_genre = albums_genre.drop_duplicates()

    albums_genre.to_csv(os.path.join(data_dir, 'albumgenre_processed.csv'), index=False)


def process_artists(data_dir):
    artists_1 = pd.read_csv(os.path.join(data_dir, 'band-member-member_name.csv'))
    artists_2 = pd.read_csv(os.path.join(data_dir, 'band-former_member-member_name.csv'))

    artists = pd.concat((artists_1,artists_2))[['artist','name']]
    artists = artists.drop_duplicates(subset=['artist','name'])

    artists_df = artists.groupby('artist').name.unique().apply(lambda x: x.tolist()).reset_index()

    artists_df['id'] = np.arange(1, len(artists_df)+1)
    cols = artists_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    artists_df = artists_df[cols]

    artists_df.to_csv(os.path.join(data_dir, 'artists_processed.csv'), index=False)


def process_artistparticipation(data_dir):
    #Read the required dataframes
    bands_df = load_or_make_df(
        os.path.join(data_dir, 'bands_processed.csv'),
        process_bands,
    )
    artists_df = load_or_make_df(
        os.path.join(data_dir, 'artists_processed.csv'),
        process_artists,
    )
    artists_former = pd.read_csv(os.path.join(data_dir, 'band-former_member-member_name.csv'))
    artists_current = pd.read_csv(os.path.join(data_dir, 'band-member-member_name.csv'))


    #Former count
    artists_former['exitCount'] = 1
    former = artists_former.groupby(['artist','band']).exitCount.count().reset_index()
    former['isActive'] = 0

    #Current count
    artists_current['exitCount'] = 1
    current = artists_current.groupby(['artist','band']).exitCount.count().reset_index()
    current['isActive'] = 1

    artists_new = pd.merge(former, current, \
                           on = ['band','artist'], \
                           how = 'outer')

    artists_new.loc[(artists_new['exitCount_x']).isnull() == True, \
                                'exitCount_x'] = 0
    artists_new.loc[(artists_new['isActive_y']) == 1, \
                                'isActive_x'] = 1

    artists_coverge = pd.merge(artists_df, artists_new, on = 'artist')
    artists_coverge = pd.merge(bands_df, artists_coverge, on='band')

    artists_participation = artists_coverge[['id_x', 'id_y', 'isActive_x', 'exitCount_x']]
    artists_participation.columns = ['id_x', 'id_y','isActive','exitCount']

    artists_participation.to_csv(os.path.join(data_dir, 'artistparticipation_processed.csv'), \
                                 index=False)
