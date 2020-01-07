from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
from nltk.corpus import stopwords
from string import punctuation
from string import digits

top_n = 15

class MRSortCountbyGenre(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_raw=self.mapper_1,
                combiner=self.combiner_1
            ),
            MRStep(
                mapper=self.mapper_2,
                reducer=self.reducer_2
            )
        ]

    def mapper_1(self, input_path, input_uri):

        # read all data from raw file
        df = pd.read_csv(input_path, dtype='unicode', sep="\t", header=0, encoding='utf-8')

        # choose only data columns of 'titleType' &  'primaryTitle'
        df1 = df[['titleType', 'primaryTitle', 'genres']]

        # choose only the rows with titleType = 'movie'
        df1 = df1.loc[df['titleType'].isin(['movie'])]
        df1 = df1[['primaryTitle', 'genres']]

        number_of_movies = df1.shape[0]

        # below code helps yielding ((genre & keyword),1) pair for every word in a primaryTitle of a movie,
        # and does it for each genre that the movie belongs to
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
                            yield ('\t'.join(pair)), 1

    def combiner_1(self, genre_and_title, counts):
        yield genre_and_title, sum(counts)

    def mapper_2(self, genre_and_title, counts):
        yield genre_and_title.split()[0], (counts, genre_and_title.split()[1])

    def reducer_2(self, genre, counts_and_titles):
        for value in (sorted(counts_and_titles, reverse=True)[:top_n]):
            yield genre, (value[1], value[0])


if __name__ == '__main__':

    # eliminate stopwords from the list of words (English, French, Spanish stopwords as well as special characters)
    eng = stopwords.words('english')
    fr = stopwords.words('french')
    esp = stopwords.words('spanish')
    stop_list = set(eng + fr + esp + list(punctuation) + list(digits))

    MRSortCountbyGenre.run()