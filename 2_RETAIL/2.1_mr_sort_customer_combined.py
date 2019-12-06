from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import pandas as pd

WORD_RE = re.compile(r"[\w']+")
top_n = 10

class MRSortCustomer(MRJob):

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

        # choose only necessary data columns, and drop the rows with missing customer information
        # missing rows could be filled using df.fillna() but then, customer id 0 ranks very high. It shouldn't.
        df1 = df[['Quantity', 'Price', 'Customer ID']].dropna(0)

        # select the data types for columns
        df1 = df1.astype({'Quantity': float, 'Price': float, 'Customer ID': int}, errors='ignore')

        complete_list = []
        for i in range(1, df1.shape[0]):
            line = str(df1.iloc[i][0]) + ',' + str(df1.iloc[i][1]) + ',' + str(df1.iloc[i][2])
            complete_list.append(line)

        for line in complete_list:
            yield None, (''.join(line))


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
            yield ('%.2f' % float(value), '%.0f' % float(customer_id))


if __name__ == '__main__':
    MRSortCustomer.run()