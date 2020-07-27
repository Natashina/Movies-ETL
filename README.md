### Movies-ETL

In the Challenge.py file there is a Challenge_Function that takes in three arguments:
- Wikipedia data
- Kaggle metadata
- MovieLens rating data (from Kaggle)

The following assumptions have been made regarding iteration of Wikipedia data.

##1. Duplicates of IMDb IDs were removed from Wikipedia dataset, that reduced the original number of rows from 7311 to 7033. 

##2. Columns that have more than 90% null values have been eliminated from the dataset. Thenumber of columns went down to 21.
[column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]

##3. The code is making these additional assumptions as regards Box office data:
To capture different expressions there are the following two variables: 

- form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on',  that represents “$123.4 million/billion.” pattern
- form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)' that represents “$123,456,789.” pattern

5485 rows match either form one or form two.
However there is other data included in different currencies and we assume that data in USD 5485 rows is sufficient for our purpose and scope.

##4. 
