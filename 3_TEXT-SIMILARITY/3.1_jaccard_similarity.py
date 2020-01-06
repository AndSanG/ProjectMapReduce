from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from nltk.corpus import stopwords
from string import punctuation, digits


class MRJaccard(MRJob):

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
            score = self.jaccard_score(queried_summary, values)
        yield None, (score, key)

    def reducer(self, _, value):
        yield max(value)

    def jaccard_score(self, list1, list2):
        set1 = set(list1)
        set2 = set(list2)
        intersection = set1.intersection(set2)
        union = set1.union(set2)
        score = float(len(intersection) / len(union))
        return score


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

    MRJaccard.run()
