from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from nltk.corpus import stopwords
from string import punctuation
from mrjob.protocol import StandardJSONProtocol

# article that we are querying for:

my_query = {"1404.5421v1": "We propose an architecture for VQA which utilizes recurrent layers to\ngenerate visual and textual attention. The memory characteristic of the\nproposed recurrent attention units offers a rich joint embedding of visual and\ntextual features and enables the model to reason relations between several\nparts of the image and question. Our single model outperforms the first place\nwinner on the VQA 1.0 dataset, performs within margin to the current\nstate-of-the-art ensemble model. We also experiment with replacing attention\nmechanisms in other state-of-the-art models with our implementation and show\nincreased accuracy. In both cases, our recurrent attention mechanism improves\nperformance in tasks requiring sequential or relational reasoning on the VQA\ndataset."}


class MRJaccard(MRJob):

    INPUT_PROTOCOL = StandardJSONProtocol

    def __init__(self, args=None):
        super().__init__(args=None)
        self.eng = stopwords.words('english')
        self.stop_list = set(self.eng + list(punctuation))
        self.queried_article_id = list(my_query)[0]
        self.query_words = set()
        for word in my_query[self.queried_article_id].lower().split():
            if word not in self.stop_list:
                self.query_words.add(word)

    def jaccard_score(self, new_article_words):
        intersection = self.query_words.intersection(new_article_words)
        union = self.query_words.union(new_article_words)
        score = float(len(intersection) / len(union))
        return score

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_0
            )
            # MRStep(
            #     mapper=self.mapper_1,
            #     reducer=self.reducer_1
            # )
        ]

    def mapper_0(self, input_path, input_uri):

        with open(input_path, 'r') as f:
            data = json.loads(f.read())

        for article in data:
            article_id = article['id']
            summary_text = article['summary'].lower()
            line = '\t'
            for word in summary_text.split():
                if word not in self.stop_list:
                    line.join(word + '\t')
            yield article_id, line

    # def mapper_1(self, new_article_id, line):
    #     new_summary = set()
    #     for word in line.split('\t'):
    #         new_summary.add(word)
    #
    #     score = self.jaccard_score(self, new_summary)
    #     yield None, (score, new_article_id)
    #
    # def reducer_1(self, _, value):
    #     yield max(value)



if __name__ == '__main__':
    MRJaccard.run()