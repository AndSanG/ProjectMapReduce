from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd


class MaxProductByQuantity(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_raw=self.mapper_preprocessor
            ),
            MRStep(
                mapper=self.mapper_1,
                combiner=self.combiner_1,
                reducer=self.reducer_1
            ),
            MRStep(
                reducer=self.reducer_2
            )
        ]

    def mapper_preprocessor(self, input_path, input_uri):

        # read all data from raw file
        df = pd.read_csv(input_path, encoding='latin-1', sep=",", header=0)

        # in assignment 2.2 we did not use dropna(0) as we did in the assignment 2.1
        # because there is no information missing about products
        # if we drop the rows with blank customer id, we will lose data of products, therefore we keep all rows.
        df1 = df[['StockCode', 'Quantity']]

        # select the data types for columns
        df1 = df1.astype({'StockCode': str, 'Quantity': int}, errors='ignore')

        for i in range(1, df1.shape[0]):
            line = str(df1.iloc[i][0]) + ',' + str(df1.iloc[i][1])
            yield None, ''.join(line)

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