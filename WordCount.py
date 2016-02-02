import sys
import pyspark
import string


class WordCount(args):

	def __init__(self, arg):
		super(WordCount, self).__init__()
		self.arg = arg


def count_words(args):
	conf_dict = {}

	execfile(args[2], {}, conf_dict)

    config = pyspark.SparkConf().setAppName("Katya's Word Count Function")
    sc = pyspark.sparkContext(conf=config)

    data = sc.textFile(conf_dict['input_data'])

    words = data.flatMap(lambda x: x.split(' '))

    lower_case = words.map(lambda x: x.tolower())

    no_punctuation = lower_case.map(lambda x: x.translate(string.maketrans("",""), string.punctuation))

    kv = no_punctuation.map(lambda x: (x,1))

    sort = kv.reduceByKey(lambda a,b: a + b).map(lambda (x,y): (y, x)).sortByKey(False)


if __name__ == 'main':
	if (len(sys.argv)) > 1:
		locals()[sys.argv[1]](sys.argv)
	else:
		raise Exception("Unknown Function")
