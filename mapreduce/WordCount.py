from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class WordCount(MRJob):
	def steps(self):
		return [MRStep(mapper=self.mapper,
						reducer=self.reducer)]

	def mapper(self,_,line):
		line = line.translate(str.maketrans("","",string.punctuation))
		words = line.split()
		for w in words:
			yield w,1

	def reducer(self,key,values):
		yield key,sum(values)

if __name__=="__main__":
	WordCount.run()
