### Movies-ETL

In the Challenge.py file there is a Challenge_Function that takes in three arguments:
- Wikipedia data
- Kaggle metadata
- MovieLens rating data (from Kaggle)

The following assumptions have been made regarding iteration of Wikipedia and Kaggle data.

1. Duplicates of IMDb IDs were removed from Wikipedia dataset, that reduced the original number of rows from 7311 to 7033. 

2. Wikipedia columns that have more than 90% null values have been eliminated from the dataset. Thenumber of columns went down to 21.
[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]

3. The code is making these additional assumptions as regards "Box office" data from Wikipedia:
  To capture different expressions there are the following two variables: 

  - form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on',  that represents “$123.4 million/billion.” pattern
  - form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)' that represents “$123,456,789.” pattern
  A function named parse_dollars turns the extracted values into a numeric value, it takes in a string and returns a floating-point number.
  The original data of Box Office column is eliminated.

  5485 rows match either form one or form two; and 1548 rows in "Box Office" column are showing null values.
  Box office column and Budget are assumed to be in the same currency. In a reality these columns include multiple currency types.

  Future version of the Challenge_Function and the enhancement would be to include code to convert multiple currency to a single currency applying foreign exchange rates.

4. The same approach and the same patterns of form_one and form_two are used to parse Budget data. 

5. Kaggle data required the correction of data types. To_numeric() method from Pandas has been used to convert numeric columns.
  
  The code is included below:
  kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
  kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
  However, in the future iterations the code might throw a error. 
  
  "Try-except" blocks can be used here to print the error message and ensure Challenge_Function will continue to work.
  
  The code would look as following:
  
  try: kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'])
  
  except: 
        
    print("Error in conversion of ID.")
    pass
  
  try: kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'])
  
  except: 
    
    print("Error in conversion of popularity.") 
    pass
