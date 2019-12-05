import pandas as pd
from nltk.corpus import stopwords
from string import punctuation
from string import digits

raw_file = 'title.basics.tsv'

# read all data from raw file
df = pd.read_csv(raw_file, dtype='unicode', sep="\t", header=0, encoding='utf-8')

# choose only data columns of 'titleType' &  'primaryTitle'
df1 = df[['titleType', 'primaryTitle', 'genres']]

# choose only the rows with titleType = 'movie'
df1 = df1.loc[df['titleType'].isin(['movie'])]
df1 = df1[['primaryTitle', 'genres']]

# eliminate stopwords from the list of words (English, French, Spanish stopwords as well as special characters)
eng = stopwords.words('english')
fr = stopwords.words('french')
esp = stopwords.words('spanish')
stop_list = set(eng + fr + esp + list(punctuation) + list(digits))

number_of_movies = df1.shape[0]
complete_list = []

for i in range(1, number_of_movies):
    line = df1.iloc[i][1]
    multiple_genres = line.split(',')
    for genre in multiple_genres:
        if genre != '\\N':
            primaryTitle = df1.iloc[i][0].lower()
            words_in_primaryTitle = primaryTitle.split()
            for word in words_in_primaryTitle:
                if word.lower() not in stop_list:
                    pair = [genre.lower(), word.lower()]
                    complete_list.append(pair)
                    print(i, ' / ', number_of_movies, pair)

complete_list.sort()

# write to file in order to pass to MR.Job
with open("input_keywords_by_genre.csv", "w") as f:
    for pair in complete_list:
        for word in pair:
            f.write(word + '\t')
        f.write('\n')
f.close()
