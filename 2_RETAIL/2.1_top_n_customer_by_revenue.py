from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd

top_n = 10


class MRSortCustomer(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_raw=self.mapper_preprocessor
            ),
            MRStep(
                mapper=self.mapper_1,
                reducer=self.reducer_1
            ),
            MRStep(
                reducer=self.reducer_2
            ),
            MRStep(
                reducer=self.reducer_3
            )
        ]

    def mapper_preprocessor(self, input_path, input_uri):

        # read all data from raw file
        df = pd.read_csv(input_path, encoding='latin-1', sep=",", header=0)

        # choose only necessary data columns, and drop the rows with missing customer information
        # however some rows have blank customerID, therefore it'd be useful to leave them out by using dropna(0)
        # we could also fill thos rows using df.fillna(), but then customer id 0 would rank very high. So we don't.
        df1 = df[['Quantity', 'Price', 'Customer ID']].dropna(0)

        # select the data types for columns
        df1 = df1.astype({'Quantity': float, 'Price': float, 'Customer ID': int}, errors='ignore')

        for i in range(1, df1.shape[0]):
            line = str(df1.iloc[i][0]) + ',' + str(df1.iloc[i][1]) + ',' + str(df1.iloc[i][2])
            yield None, ''.join(line)

    def mapper_1(self, _, line):
        a = line.split(',')
        qty = float(a[0])
        price = float(a[1])
        customer_id = str(a[2])
        yield customer_id, (qty * price)

    def reducer_1(self, customer_id, value):
        yield customer_id, sum(value)

    def reducer_2(self, key, values):
        yield None, (sum(values), key)

    def reducer_3(self, _, value_and_customer):
        for value, customer_id in (sorted(value_and_customer, reverse=True)[:top_n]):
            yield ('%.2f' % float(value), '%.0f' % float(customer_id))


if __name__ == '__main__':
    MRSortCustomer.run()