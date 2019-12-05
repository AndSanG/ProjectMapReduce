from mrjob.job import MRJob
from mrjob.step import MRStep


class MaxProductByQuantity(MRJob):

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
        stock_code = str(a[0])
        qty = int(a[1])
        yield stock_code, qty

    def combiner_1(self, stock_code, value):
        yield stock_code, sum(value)

    def reducer_1(self, key, values):
        yield None, (sum(values), key)

    def reducer_2(self, _, value):
        yield max(value)


if __name__ == '__main__':
    MaxProductByQuantity.run()