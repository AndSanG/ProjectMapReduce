from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
from nltk.corpus import stopwords
from string import punctuation, digits

top_n = 50


class MRSortCount(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_raw=self.mapper_raw,
                combiner=self.combine_word_counts,
                reducer=self.reducer_sum_word_counts
            ),
            MRStep(
                reducer=self.reduce_sort_counts
            )
        ]

    def mapper_raw(self, input_path, input_uri):

        # read all data from raw file
        df = pd.read_csv(input_path, dtype='unicode', sep="\t", header=0, encoding='utf-8')

        # choose only data columns of 'titleType' &  'primaryTitle'
        df1 = df[['titleType', 'primaryTitle']]

        # choose only the rows with titleType = 'movie' or 'short'
        df1 = df1.loc[df['titleType'].isin(['movie', 'short'])]
        df1 = df1[['primaryTitle']]

        # create a list of words with the filtered movies
        number_of_movies = df1.shape[0]
        for i in range(number_of_movies):
            movie_name = (df1.iloc[i][0])
            words = str(movie_name).lower().split()
            for word in words:
                if word not in stop_list:
                    yield word.lower(), 1

    def combine_word_counts(self, word, counts):
        yield word, sum(counts)

    def reducer_sum_word_counts(self, key, values):
        yield None, (sum(values), key)

    def reduce_sort_counts(self, _, word_counts):
        for count, key in (sorted(word_counts, reverse=True)[:top_n]):
            yield ('%d' % int(count), key)

if __name__ == '__main__':

    # eliminate stopwords from the list of words (English, French,1.1_top_n_keywords_imdb.py Spanish stopwords as well as special characters)
    eng = stopwords.words('english')
    fr = stopwords.words('french')
    esp = stopwords.words('spanish')
    stop_list = set(eng + fr + esp + list(punctuation) + list(digits))

    MRSortCount.run()