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
    nameFix = lambda x: x.replace('"', "'") if (isinstance(x, str)) else ""

    bands_df["band"] = bands_df.band.apply(nameFix)
    bands_df["bandname"] = bands_df.bandname.apply(nameFix)
    bands_df["start"] = bands_df.start.apply(dateFix)
    bands_df["end"] = bands_df.end.apply(dateFix)


    bands_df.columns = ['id', 'dbpedia', 'name', 'start', 'end']

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
    #band_album_genre.columns = ['dbpedia', 'album_name', 'release_date', 'abstract', 'duration', 'sold']

    band_album.columns = ['dbpedia', 'album_name', 'release_date', 'abstract', 'duration', 'sold']
    band_album_genre.columns = ['dbpedia', 'album', 'album_name', 'genre', 'release_date', 'abstract',
       'duration', 'sold']

    albums_df = pd.merge(band_album, bands_df, on='dbpedia', how='outer')
    albums_df = albums_df.drop_duplicates(subset=['album_name', 'id'])
    albums_df['albumId'] = np.arange(1, len(albums_df) + 1)


    #album_df :
    albums_df_all = pd.merge(albums_df, band_album_genre, on=['dbpedia', 'album_name'])
    albums_df_all['album_name'] =  albums_df_all.album_name.apply(lambda row: ' '.join([re.sub('[^A-Za-z0-9]+', '', k) for k in row.split(' ')]))
    fixGenre = lambda row: row.split("/")[-1].replace("_", " ")
    albums_df_all['genre'] = albums_df_all.genre.apply(fixGenre)

    #Album data
    albums_df = albums_df_all.drop_duplicates(subset=['albumId'])[['albumId','album_name', 'album']]
    albums_df.columns = ['id','name','dbpedia']

    #Album band association data
    albums_bands = albums_df_all.drop_duplicates(subset=['album'])[['id','albumId']]
    albums_bands.columns = ['bands','albums']

    #Genre data
    genres_df = pd.DataFrame({'genre': albums_df_all['genre'].unique(), 'id': np.arange(1, len(albums_df_all['genre'].unique())+1)})

    #Album Genre association data
    albums_genres = albums_df_all.drop_duplicates(subset=['album', 'genre'])[['albumId', 'genre']]
    albums_genres = pd.merge(albums_genres, genres_df, on = 'genre')[['albumId','id']]

    albums_genres.columns = ['albums','genres']




    albums_df.to_csv(os.path.join(data_dir, 'albums_processed.csv'), index=False)
    albums_bands.to_csv(os.path.join(data_dir, 'bands_albums_processed.csv'), index=False)
    albums_genres.to_csv(os.path.join(data_dir, 'albums_genres_processed.csv'), index=False)
    genres_df[['id','genre']].to_csv(os.path.join(data_dir, 'genres_processed.csv'), index=False)

    # #Get arrays of sales, launchDate, durations
    # albums_launchDate = albums_df.groupby(['URI']).release.unique().apply(lambda x: x.tolist()).reset_index()
    # albums_sold = albums_df.groupby(['URI']).sold.unique().apply(lambda x: x.tolist()).reset_index()
    # albums_duration = albums_df.groupby(['URI']).duration.unique().apply(lambda x: x.tolist()).reset_index()

    # #Merge and choose columns
    # albums_df_temp = pd.merge(pd.merge(albums_launchDate,albums_sold),albums_duration)
    # albums_df = pd.merge(albums_df.drop_duplicates('URI'), albums_df_temp, suffixes=['_noArray','_array'], on='URI')
    # albums_df = albums_df[['URI','bandID','name','release_array', 'duration_array', 'sold_array', 'abstract']]

    # albums_df['id'] = np.arange(1, len(albums_df) + 1)
    # cols = albums_df.columns.tolist()
    # cols = cols[-1:] + cols[:-1]
    # albums_df = albums_df[cols]

    # def fix_sales(x):
    #     if isinstance(x, int):
    #         return x
    #     if isinstance(x, str) and "rowspan" in x:
    #         return int(re.findall(r"[0-9,0-9]+", x)[-1].replace(",", ""))
    #     else:
    #         return x

    # _fix_sales = lambda x: list(map(fix_sales, x))
    # albums_df["sold_array"] = albums_df["sold_array"].apply(_fix_sales)

    # albums_df.columns = ['id', 'dbpedia','bandID','name','release', 'duration', 'sold', 'abstract']
    # #This is required since the names are not correct

    # album_band_association_df = albums_df[['id', 'bandID', 'dbpedia']]

    # albums_df = albums_df[['id'] + constitution]


    # albums_df.to_csv(os.path.join(data_dir, 'albums_processed.csv'), index=False)
    # album_band_association_df.to_csv(os.path.join(data_dir, 'albums_bands_processed.csv'), index=False)



def process_albumgenre(data_dir, constitution):
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

    print(albums_df.iloc[0:1])
    print(bands_df.iloc[0:1])
    print(band_album_genre.iloc[0:1])
    print(band_album.iloc[0:1])
    print("----------------------------------------------------------")


    band_album_genre.columns = ['dbpedia', 'album', 'album_name', 'genre', 'release_date', 'abstract',
       'duration', 'sold']
    band_album.columns = ['dbpedia', 'album_name', 'release_date', 'abstract', 'duration', 'sold']

    albums_df = pd.merge(band_album, bands_df, on='dbpedia', how='outer')
    albums_df_all = pd.merge(albums_df, band_album_genre, on=['dbpedia', 'release_date', 'duration', 'album_name'])


    albums_df = pd.read_csv(os.path.join(data_dir, 'albums_bands_processed.csv'))
    albums_df_genre = albums_df_all[['id','dbpedia', 'album', 'genre', 'album_name']]



    albums_df_genre.columns = ['bandID', 'dbpedia', 'genre', 'name']

    albums_genre = pd.merge(albums_df_genre, albums_df, on=['dbpedia','bandID', 'name'])

    albums_genre = albums_genre.drop_duplicates()
    albums_genre = albums_genre[['id','bandID','genre']]
    print(albums_genre.iloc[0:1])
    albums_genre.columns = ['albumID','bandID','name']

    fixGenre = lambda row: row.split("/")[-1].replace("_", " ")

    albums_genre['genre'] = albums_genre.genre.apply(fixGenre)

    albums_genre = albums_genre.drop_duplicates()

    album_genre_association = album_genre_association[['albumID','genre']]
    albums_genre = album_genre[constitution]

    album_genre_association.to_csv(os.path.join(data_dir, 'albums_genre_processed.csv'), index=True)


def process_artists(data_dir):
    artists_1 = pd.read_csv(os.path.join(data_dir, 'band-member-member_name.csv'))
    artists_2 = pd.read_csv(os.path.join(data_dir, 'band-former_member-member_name.csv'))

    artists = pd.concat((artists_1,artists_2))[['artist','name']]
    artists = artists.drop_duplicates(subset=['artist','name'])

    artists['name'] = artists.name.apply(lambda row: ' '.join([re.sub('[^A-Za-z0-9]+', '', k) for k in str(row).split(' ')]))

    artists_df = artists.groupby('artist').name.unique().apply(lambda x: ";".join(filter(lambda y: type(y) == str, x.tolist()))).reset_index()

    artists_df['id'] = np.arange(1, len(artists_df)+1)
    cols = artists_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]

    artists_df = artists_df[cols]
    artists_df.columns = ['id', 'dbpedia','name']

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

    artists_new.columns = ['dbpedia', 'band', 'exitCount_x', 'isActive_x', 'exitCount_y', 'isActive_y']
    artists_coverge = pd.merge(artists_df, artists_new, on = 'dbpedia')
    artists_coverge = pd.merge(bands_df, artists_coverge, left_on = 'dbpedia', right_on='band')

    artists_participation = artists_coverge[['id_x', 'id_y', 'isActive_x', 'exitCount_x']]
    artists_participation.columns = ['bands', 'artists','isActive','exitCount']

    artists_participation.to_csv(os.path.join(data_dir, 'bands_artists_processed.csv'), \
                                 index=False)
