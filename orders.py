from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRSpent_amount_per_Customer(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_amount,
                   reducer=self.reducer_sum_amount),
            MRStep(mapper=self.mapper_make_counts_key,
                   reducer = self.reducer_output_amounts)
        ]

    def mapper_get_amount(self, _, line):
        (customer, item, amount) = line.split(",")
        yield customer, float(amount)
        
    def reducer_sum_amount(self, customer, amount):
        yield customer, sum(amount)
        
    def mapper_make_counts_key(self, customer, amount):
        yield '%05.02f'%float(amount), customer

    def reducer_output_amounts(self, amount, customer):
        for customer in customer:
            yield amount, customer



if __name__ == '__main__':
    MRSpent_amount_per_Customer.run()