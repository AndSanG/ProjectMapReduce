from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")
top_n = 10

class MRSortCustomer(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_1,
                combiner=self.combiner_1,
                reducer=self.reducer_1
            ),
            MRStep(
                reducer=self.reducer_2
            )
        ]

    def mapper_1(self, _, line):
        a = line.split(',')
        qty = float(a[0])
        price = float(a[1])
        customer_id = str(a[2])
        yield customer_id, (qty * price)

    def combiner_1(self, customer_id, value):
        yield customer_id, sum(value)

    def reducer_1(self, key, values):
        yield None, (sum(values), key)

    def reducer_2(self, _, value_and_customer):
        for value, customer_id in (sorted(value_and_customer, reverse=True)[:top_n]):
            yield ('%.2f' % float(value), int(customer_id))


if __name__ == '__main__':
    MRSortCustomer.run()