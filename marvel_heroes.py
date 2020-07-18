from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularHero(MRJob):
    
    def configure_args(self):
        super(MostPopularHero, self).configure_args()
        self.add_file_arg('--heroes', help = "Path to Marvel-Names")
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_friends,
                   reducer_init = self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_friends(self, _, line):
        inputs = line.split()
        superhero = inputs[0]
        friends = len(inputs) -1
        yield superhero, friends
            

#This mapper does nothing; it's just here to avoid a bug in some
#versions of mrjob related to "non-script steps." Normally this
#wouldn't be needed.
    def mapper_passthrough(self, hero, friends):
        yield '%04d'%int(friends), hero
        
    def reducer_init(self):
        self.hero_names = {}
        with open("Marvel-Names.txt") as m:
            for line in m:
                fields = line.split(" ")
                self.hero_names[fields[0]] = fields[1]

    def reducer_count_ratings(self, superhero, friends):
        yield self.hero_names[superhero], sum(friends)

    def reducer_find_max(self, friends, hero):
        for hero in hero:
            yield hero, friends

if __name__ == '__main__':
    MostPopularHero.run()
