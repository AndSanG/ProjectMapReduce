from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from nltk.corpus import stopwords
from string import punctuation


query_id = '1802.00209v1'


class MrJaccard(MRJob):

    def __init__(self, args=None):
        super().__init__(args=None)
        self.eng = stopwords.words('english')
        self.stop_list = set(self.eng + list(punctuation))

    def mapper(self, input_path, input_uri):
        with open(input_path) as f:
            data = json.loads(f.read())

        def get_summary(file, art_id):
            for article in file:
                if article['id'] == art_id:
                    set_of_words_in_summary = set(article['summary'].lower().split())
                    return set_of_words_in_summary

        def get_jaccard_score(set1, set2):
            intersection = set1.intersection(set2)
            union = set1.union(set2)
            j_score = float(len(intersection) / len(union))
            return j_score

        query_summary = get_summary(data, query_id)
        query_set = [item for item in query_summary if item not in self.stop_list]

        for article in data:
            article_id = article['id']
            new_set = get_summary(data, article_id)
            score = get_jaccard_score(new_set, query_set)
            yield None, (score, article_id)

    def reducer(self, _, value):
        yield max(value)

if __name__ == '__main__':
    MrJaccard.run()