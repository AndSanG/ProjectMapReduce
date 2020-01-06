from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from nltk.corpus import stopwords
from string import punctuation, digits
import numpy as np


class MRCosine(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_raw=self.mapper_raw,
            ),
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer,
            )
        ]

    def mapper_raw(self, input_path, input_uri):
        with open(input_path) as f:
            data = json.loads(f.read())
            for article in data:
                words_summary = set(article['summary'].lower().split())
                filtered_summary = [word for word in words_summary if word not in stop_list]
                yield article['id'], list(filtered_summary)

    def mapper(self, key, values):
        score = 0
        if key != queried_article_id:
            score = self.cosine_score(queried_summary, values)
        yield None, (score, key)

    def reducer(self, _, value):
        yield max(value)

    def cosine_score(self, list1, list2):

        vocab = {}
        i = 0

        for word in list1:
            if word not in vocab:
                vocab[word] = i
                i += 1

        for word in list2:
            if word not in vocab:
                vocab[word] = i
                i += 1

        # create a numpy array (vector) for each input, filled with zeros
        a = np.zeros(len(vocab))
        b = np.zeros(len(vocab))

        # loop through each input and create a corresponding vector for it
        # this vector counts occurrences of each word in the dictionary
        for word in list1:
            index = vocab[word]     # get index from dictionary
            a[index] += 1           # increment count for that index

        for word in list2:
            index = vocab[word]
            b[index] += 1

        cosine_score = np.dot(a, b) / np.sqrt(np.dot(a, a) * np.dot(b, b))

        return cosine_score


if __name__ == '__main__':
    eng = stopwords.words('english')
    stop_list = set(eng + list(punctuation) + list(digits))
    queried_article_id = "1603.03827v1"

    # one-time processing to access the content of the queried article,
    # to be able to compare it with other articles in the arhive.json file
    with open('arxivData.json') as file:
        data_ = json.loads(file.read())
        for article_ in data_:
            if article_['id'] == queried_article_id:
                queried_summary = article_['summary'].lower().split()
                queried_summary = [word for word in queried_summary if word not in stop_list]

    MRCosine.run()
