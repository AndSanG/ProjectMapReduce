import nltk
import pandas as pd
from nltk.corpus import stopwords
from string import punctuation, digits

raw_file = 'title.basics.tsv'

# read all data from raw file
df = pd.read_csv(raw_file, dtype='unicode', sep="\t", header=0, encoding='utf-8')

# choose only data columns of 'titleType' &  'primaryTitle'
df1 = df[['titleType', 'primaryTitle']]

# choose only the rows with titleType = 'movie' or 'short'
df1 = df1.loc[df['titleType'].isin(['movie', 'short'])]
df1 = df1[['primaryTitle']]

# create a list of words with the filtered movies
all_words = []
number_of_movies = df1.shape[0]
for i in range(number_of_movies):
    movie_name = (df1.iloc[i][0])
    words = movie_name.split()
    # print(i)
    for word in words:
        all_words.append(word.lower())

# eliminate stopwords from the list of words (English, French, Spanish stopwords as well as special characters)
eng = stopwords.words('english')
fr = stopwords.words('french')
esp = stopwords.words('spanish')
stop_list = set(eng + fr + esp + list(punctuation) + list(digits))

filtered_words = [word for word in all_words if word not in stop_list]

# write to file in order to pass to MR.Job
with open("input_primaryTitles.csv", "w") as f:
    for word in filtered_words:
        f.write(str(word) + '\n')
f.close()