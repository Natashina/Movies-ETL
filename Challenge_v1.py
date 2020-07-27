#!/usr/bin/env python
# coding: utf-8

# In[58]:


import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
import psycopg2
import time


# In[59]:


file_dir = 'C:/Users/Natalia/Desktop/Class/Movies-ETL/'


# In[60]:


f'{file_dir}wikipedia.movies.json'


# In[61]:


with open(f'{file_dir}/wikipedia.movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)


# In[62]:


kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv')


# In[63]:


ratings = pd.read_csv(f'{file_dir}ratings.csv')


# In[64]:


# A function to make a copy of the movie and new local variable called movie
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
    
    
    return movie


# In[65]:


def parse_dollars(s):
# updated form_one
    
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan


# In[66]:


# make a function that fills in missing data for a column pair and then drops the redundant column
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)


# In[69]:


def Challenge_Function(wiki_movies_raw, kaggle_metadata, ratings):
    #Adding that filter to remove shows to our list comprehension
    wiki_movies = [movie for movie in wiki_movies_raw
               if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
    
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    
    # Drop duplicates of IMDb IDs
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    # list of columns that have less than 90% null values and use those to trim down our dataset
    [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    
    # data series that drops missing values
    box_office = wiki_movies_df['Box office'].dropna() 
    
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    #create variables one and two that equals to reg ex string
    #create a new column in the dataframe
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'    
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # Drop "Box ofice" column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)

    # create a budget variable
    budget = wiki_movies_df['Budget'].dropna()

    # convert list to string
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)

    # remove values between $ and hyphen
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # Budget that matches form one and form two
    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)

    # Remove the citation references
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    # Parsing the budget values
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)

    # Dropping original Budget column
    wiki_movies_df.drop('Budget', axis=1, inplace=True)

    # Parse Release Date: make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings:
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    # Parse the dates using the built-in to_datetime() method in Pandas
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    # Parse Running Time
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # Extract values
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')

    # Use the to_numeric() method and set the errors argument to 'coerce'.
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)

    # convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero, and save the output to wiki_movies_df
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)

    # Drop Running time
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

    # keep rows where the adult column is False, and then drop the adult column.
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

    # create a Boolean column
    kaggle_metadata['video'] == 'True'
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

    # use the to_numeric() method to ensure that data can be converted to numbers
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    try: kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    except: 
        print("error in conversion of ID")
        pass
    
    try: kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    except: 
        print("error in conversion of popularity") 
        pass
        
    # convert release_date to datetime
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    # to convert it to a datetime data type
    pd.to_datetime(ratings['timestamp'], unit='s')

    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    # convert to tuples
    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)

    # drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # Video has only one value
    movies_df['video'].value_counts(dropna=False)

    # reorder the columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]

    # rename the columns
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)

    # to use a groupby on the “movieId” and “rating” columns and take the count for each group
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1) 

    # pivot this data so that movieId is the index, the columns are all the rating values, and the rows are the counts for each rating value
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                 .rename({'userId':'count'}, axis=1)                 .pivot(index='movieId',columns='rating', values='count')

    # rename the columns so they’re easier to understand
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

    # merge the rating counts into movies_df
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    # Fill in misssing values with zeros
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

    # "postgres://[postgres]:[db_password]@[location]:[port]/[database]"

    # from config import db_password

    # db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

    # engine = create_engine(db_string)

    # movies_df.to_sql(name='movies', con=engine)

    # # Import the Ratings Data

    # rows_imported = 0
    # # get the start_time from time.time()
    # start_time = time.time()
    # for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
    #     print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
    #     data.to_sql(name='ratings', con=engine, if_exists='append')
    #     rows_imported += len(data)

    #     # add elapsed time to final print out
    #     print(f'Done. {time.time() - start_time} total seconds elapsed')
    
    return(movies_with_ratings_df)


# In[70]:


Challenge_Function(wiki_movies_raw, kaggle_metadata, ratings)



# %%
