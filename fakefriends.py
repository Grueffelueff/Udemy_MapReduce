from mrjob.job import MRJob
import statistics

class MRavgFriends(MRJob):
    def mapper(self, key, line):
        (userID, name, age, no_friends) = line.split(',')
        yield age, float(no_friends)

    def reducer(self, age, no_friends):
        yield age, statistics.mean(no_friends)

if __name__ == '__main__':
    MRavgFriends.run()
