
"""The classic MapReduce job: count the frequency of words.
"""
from mrjob.job import MRJob
from mrjob.py2 import to_unicode
from mrjob.util import log_to_stream, log_to_null
import re

import pandas as pd
from nltk.corpus import stopwords
from string import punctuation, digits

import logging
log = logging.getLogger(__name__)

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):

    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        log_to_stream(name='mrjob', debug=verbose, stream=stream)
        log_to_stream(name='__main__', debug=verbose, stream=stream)

    def mapper_raw(self, input_path, input_uri):

        raw_file = input_path

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

        log.info(filtered_words)


        yield None,filtered_words

    '''
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))
'''

if __name__ == '__main__':
    MRWordFreqCount.run()
