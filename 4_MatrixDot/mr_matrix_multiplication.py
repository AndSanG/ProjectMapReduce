from mrjob.job import MRJob, MRStep
from numpy import loadtxt, zeros, savetxt
import sys

class MRMatrixMultiplication(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper_raw=self.mapper_raw,
                combiner=self.combiner_mult,
                reducer=self.reducer_mult
            ),
            MRStep(
                combiner=self.combiner_sum,
                reducer=self.reducer_sum
            )
        ]

    def mapper_raw(self, input_path, input_uri):
        # name of the input file, either A matrix or B matrix
        name = input_uri.split("/")[-1]
        # name of the A matrix file
        arg1 = sys.argv[1]
        matrix = loadtxt(input_path)

        # per each A element is created as many key value pairs as the number of cols of B.
        if name == arg1:
            for row in range(matrix.shape[0]):
                for col in range(matrix.shape[1]):
                    for k in range(q):
                        yield (row, col, k), (matrix[row][col])

        # per each B element is created as many key value pairs as the number of rows of A.
        else:
            for row in range(matrix.shape[0]):
                for col in range(matrix.shape[1]):
                    for k in range(m):
                        yield (k,row, col), (matrix[row][col])

    # combiner added for improvement in the map reduce
    def combiner_mult(self, key, values):
        # two values with the same key(one per matrix) are multiplied.
        mult = 1
        for value in values:
            mult*=value
        yield key,mult

    def reducer_mult(self, key, values):
        # two values with the same key(one per matrix) are multiplied.
        mult = 1
        for value in values:
            mult*=value
        yield (key[0],key[2]),mult

    # combiner added for improvement in the map reduce
    def combiner_sum(self, key, values):
        # all the values belonging to the same row,col were multiplied and in this step sum.
        sum_ = sum(values)
        C[key[0]][key[1]] = sum_
        yield key, sum_

    def reducer_sum(self, key, values):
        # all the values belonging to the same row,col were multiplied and in this step sum.
        sum_ =  sum(values)
        C[key[0]][key[1]] = sum_
        yield key, sum_

if __name__ == '__main__':
    #load matrixes to check the shapes
    A = loadtxt(sys.argv[1])
    B = loadtxt(sys.argv[2])
    m = A.shape[0]
    p = A.shape[1]
    p1 = B.shape[0]
    q = B.shape[1]
    C = zeros([m, q])

    #check matrix multiplication shapes
    if(p==p1):
        MRMatrixMultiplication.run()
        savetxt('C.txt', C)
    else:
        CRED = '\033[91m'
        CEND = '\033[0m'
        print(CRED + "Error, Matrix shapes does not conform matrix multiplication requirements" + CEND)




