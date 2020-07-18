from mrjob.job import MRJob
import re

REGEXP = re.compile(r"[\w']+")

class MRwordcount(MRJob):
    def mapper(self, key, line):
        (words_in_line) = line.split()
        for word in words_in_line:
            yield word.lower(), 1

    def reducer(self, words, occ):
        yield words, sum(occ)
        
class MRwordcount_regex(MRJob):
    def mapper(self, key, line):
        words = REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, words, occ):
        yield words, sum(occ)
        
if __name__ == '__main__':
    #MRwordcount.run()
    MRwordcount_regex.run()