from mrjob.job import MRJob


class MRMatrixMultiplication(MRJob):

    def mapper(self, _, line):
        yield line, 1
    '''
    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))
    '''


if __name__ == '__main__':
    MRMatrixMultiplication.run()