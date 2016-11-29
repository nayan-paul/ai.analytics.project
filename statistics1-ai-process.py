import traceback
from pyspark import SparkContext
from collections import namedtuple
from datetime import datetime
from pyspark.sql import SQLContext,Row
# This process is algo1 for calculating counts of clicks(destination_subid) per hour for combination of offer_id + offer_advertiser_id

#http://offer.camp/stream-odd
#spark-submit --master local --jars /tmp/com.mysql.jdbc_5.1.5.jar  statistics1-ai-process.py
#spark-submit --master yarn-client --jars /tmp/com.mysql.jdbc_5.1.5.jar statistics1-ai-process.py

#COMMON
currentTime = datetime.utcnow()

fields=('type','key_dt','offer_id','offer_advertiser_id','destination_subid','count_of_clicks')
statsDTO = namedtuple('statsDTO',fields)

properties ={"driver": "com.mysql.jdbc.Driver"}
#######################################################################################################
#M1
def M1(line):
	try:
		tokens = line.split('\t')
		dt = str(currentTime.year)+'-'+str(currentTime.month).zfill(2) +'-'+str(currentTime.day).zfill(2) +'-'+str(currentTime.hour).zfill(2)
		return statsDTO(type=tokens[0],key_dt=dt,offer_id=tokens[6],offer_advertiser_id=tokens[5],destination_subid=tokens[2],count_of_clicks=1)
	#try
	except:
		traceback.print_exc()
	#except
###############################################################################################################
#M2
def M2(tup):
	try:
		k = tup[0]
		v = tup[1]
		return (v.key_dt,v.type,v.offer_id,v.offer_advertiser_id,v.destination_subid,v.count_of_clicks)
	#try
	except:
		traceback.print_exc()
	#except
##############################################################################################################
#P1
def P1(rdd):
	try:
		print rdd.type +','+str(rdd.offer_id)+','+str(rdd.offer_advertiser_id)+","+rdd.destination_subid
	#try
	except:
		traceback.print_exc()
	#except
###############################################################################################################	
if __name__=='__main__':
	context = SparkContext(appName="Statistics 1 Process")
	sqlContext = SQLContext(context)
	try:	
		
		path1 = 's3a://v2clicklog/offer-stream-even/'+str(currentTime.year)+'/'+str(currentTime.month).zfill(2) +'/'+str(currentTime.day).zfill(2) +'/'+str(currentTime.hour).zfill(2) +"/*.gz"
		path2 = 's3a://v2clicklog/offer-stream-odd/'+str(currentTime.year)+'/'+str(currentTime.month).zfill(2) +'/'+str(currentTime.day).zfill(2) +'/'+str(currentTime.hour).zfill(2) +"/*.gz"
		
		context._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAJ6GZUJPYDANGPCEQ")
		context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
		
		if currentTime.day%2 == 1:
			inputRDD = context.textFile(path1)
		#if
		else:
			inputRDD = context.textFile(path2)
		#if
		loadedPairRDD = inputRDD.map(M1).map(lambda p : ((p.offer_id+"_"+p.offer_advertiser_id),p)).filter(lambda (k,v): len(k)>3)
		
		def R1(acc,p):
			val = acc.count_of_clicks + p.count_of_clicks
			return statsDTO(type=acc.type,key_dt=acc.key_dt,offer_id=acc.offer_id,offer_advertiser_id=acc.offer_advertiser_id,destination_subid='NA',count_of_clicks=val)
		processedPairRDD = loadedPairRDD.reduceByKey(R1)
		
		rowRDD = processedPairRDD.map(M2)
		processedDF=rowRDD.toDF(['key_dt','key_type','offer_id','offer_advertiser_id','destination_subid','count_of_clicks'])
		
		processedDF.write.jdbc(url='jdbc:mysql://aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com:3306/aitracker?user=aitracker&password=aitracker',table='v2_s3_stats',mode='overwrite',properties=properties)		
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'End of process'
	#finally
#if