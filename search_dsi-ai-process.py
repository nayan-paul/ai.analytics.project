import traceback
from collections import namedtuple
from pyspark import SparkContext
from datetime import datetime
import sys
#This process is to search for a destination_subid in S3 bucket

#spark-submit --master local search_dsi-ai-process.py z1611fe7-284d-63bf-b806-01604e604974
#spark-submit --master yarn-cluster search_dsi-ai-process.py z1611fe7-284d-63bf-b806-01604e604974

#COMMON
fields=('type','offer_id','offer_advertiser_id','destination_subid','count_of_clicks')
statsDTO = namedtuple('statsDTO',fields)
##############################################################################################################
#M1
def M1(line):
	try:
		tokens = line.split('\t')
		return statsDTO(type=tokens[0],offer_id=tokens[6],offer_advertiser_id=tokens[5],destination_subid=tokens[2],count_of_clicks=1)
	#try
	except:
		traceback.print_exc()
	#except
#M1
##############################################################################################################
if __name__=='__main__':
	context = SparkContext(appName='Search destination Subid Process')
	try:
		currentTime = datetime.utcnow()
		path1 = 's3a://v2clicklog/offer-stream-even/'+str(currentTime.year)+'/'+str(currentTime.month).zfill(2) +'/'+str(currentTime.day).zfill(2) +"/*/*.gz"
		path2 = 's3a://v2clicklog/offer-stream-odd/'+str(currentTime.year)+'/'+str(currentTime.month).zfill(2) +'/'+str(currentTime.day).zfill(2) +"/*/*.gz"
		
		context._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAJ6GZUJPYDANGPCEQ")
		context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
		
		if currentTime.day%2 == 1:
			inputRDD = context.textFile(path1)
		#if
		else:
			inputRDD = context.textFile(path2)
		#if
		searchData = sys.argv[1]
		
		loadedRDD = inputRDD.map(M1)
		
		def S1(x):
			if x.destination_subid==searchData:
				return x
			#if
		searchResult = loadedRDD.filter(S1)
		
		for rdd in searchResult.collect():
			print '=============================='+str(rdd.offer_id)+','+str(rdd.offer_advertiser_id)
		#for	
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'End of Process...'
	#finally
#if