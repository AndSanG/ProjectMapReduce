from mrjob.job import MRJob
from mrjob.step import MRStep
import re

import pandas as pd
from nltk.corpus import stopwords
from string import punctuation, digits


WORD_RE = re.compile(r"[\w']+")
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

    def mapper_extract_words(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combine_word_counts(self, word, counts):
        yield word, sum(counts)

    def reducer_sum_word_counts(self, key, values):
        yield None, (sum(values), key)

    def reduce_sort_counts(self, _, word_counts):
        for count, key in (sorted(word_counts, reverse=True)[:top_n]):
            yield ('%d' % int(count), key)

    def mapper_raw(self, input_path, input_uri):

        # read all data from raw file
        df = pd.read_csv(input_path, dtype='unicode', sep="\t", header=0, encoding='utf-8')

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

        for word in filtered_words:
            yield word.lower(), 1

if __name__ == '__main__':
    MRSortCount.run()