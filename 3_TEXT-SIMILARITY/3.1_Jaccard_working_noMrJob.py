import json
import time
from nltk.corpus import stopwords
from string import punctuation


query_id = '1802.00209v1'
input_path = 'arxivData.json'

eng = stopwords.words('english')
stop_list = set(eng + list(punctuation))

with open(input_path) as f:
    data = json.loads(f.read())


def get_summary(article_id):
    for article in data:
        if article['id'] == article_id:
            set_of_words_in_summary = set(article['summary'].lower().split())
            return set_of_words_in_summary


set_query = get_summary(query_id)
set_query = [item for item in set_query if item not in stop_list]


def jaccard_score(set1, set2):
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    score = float(len(intersection) / len(union))
    return score, len(intersection), intersection


start = time.time()
listB = []
for index, article in enumerate(data):
    article_id = article['id']
    new_set = get_summary(article_id)
    score, length, inters = jaccard_score(new_set, set_query)
    print(index, article_id, score)
    listB.append((score, article_id, length, inters))

listB.sort(reverse=True)
for el in listB[:100]:
    print(el)

end = time.time()
print('time: ', end-start)

