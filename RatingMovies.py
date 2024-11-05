from mrjob.job import MRJob
from mrjob.job import MRStep

class RatingMovies(MRJob):
    def mapper(self, _, line):
        (userId, movieId, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer)
        ]

if __name__ == '__main__':
    RatingMovies().run()