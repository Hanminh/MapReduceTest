from mrjob.job import MRJob
from mrjob.job import MRStep

class TopMovies(MRJob):
    def mapper(self, _, line):
        (userId, movieId, rating, timestamp) = line.split('\t')
        yield movieId, 1 # return key, value pair

    def reducer(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sort(self, count, movies):
        for movie in movies:
            yield count, movie

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.reducer),
            MRStep(reducer = self.reducer_sort)
        ]

if __name__ == '__main__' :
    TopMovies.run()