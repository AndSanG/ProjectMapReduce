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
        name = input_uri.split("/")[-1]
        arg1 = sys.argv[1]
        matrix = loadtxt(input_path)

        if name == arg1:
            for row in range(matrix.shape[0]):
                for col in range(matrix.shape[1]):
                    for k in range(p):
                        yield (row, col, k), (matrix[row][col])
        else:
            for row in range(matrix.shape[0]):
                for col in range(matrix.shape[1]):
                    for k in range(m):
                        yield (k,row, col), (matrix[row][col])

    def combiner_mult(self, key, values):
        mult = 1
        for value in values:
            mult*=value
        yield key,mult

    def reducer_mult(self, key, values):
        mult = 1
        for value in values:
            mult*=value
        yield (key[0],key[2]),mult

    def combiner_sum(self, key, values):
        sum_ = sum(values)
        C[key[0]][key[1]] = sum_
        yield key, sum_

    def reducer_sum(self, key, values):
        sum_ =  sum(values)
        C[key[0]][key[1]] = sum_
        yield key, sum_

if __name__ == '__main__':
    A = loadtxt(sys.argv[1])
    B = loadtxt(sys.argv[2])
    m = A.shape[0]
    n = A.shape[1]
    n1 = B.shape[0]
    p = B.shape[1]
    C = zeros([m,p])

    if(n==n1):
        MRMatrixMultiplication.run()
        savetxt('C.txt', C)
    else:
        CRED = '\033[91m'
        CEND = '\033[0m'
        print(CRED + "Error, Matrix shapes does not conform matrix multiplication requirements" + CEND)




