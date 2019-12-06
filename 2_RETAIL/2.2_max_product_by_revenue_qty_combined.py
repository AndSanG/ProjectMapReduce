from mrjob.job import MRJob
from mrjob.step import MRStep


class MaxProductByRevenue(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_rev,
                combiner=self.combiner_rev,
                reducer=self.reducer_rev
            ),
            MRStep(
                mapper=self.mapper_qty,
                combiner=self.combiner_qty,
                reducer=self.reducer_qty
            ),
            MRStep(
                reducer=self.reducer_2
            )
        ]

    def mapper_rev(self, _, line):
        a = line.split(',')
        stock_code = str(a[0])
        qty = int(a[1])
        price = float(a[2])
        yield stock_code, ((str(qty * price) + '\t' + str(qty)))

    def combiner_rev(self, stock_code, line):
        b = line.split('\t')
        qty_total = int(b[0])
        price_total = float(b[1])
        yield stock_code, sum(qty_total)
        yield stock_code, sum(price_total)

    def reducer_rev(self, key, values):
        yield None, (sum(values), key)

    def reducer_2(self, _, value):
        yield max(value)


if __name__ == '__main__':
    MaxProductByRevenue.run()