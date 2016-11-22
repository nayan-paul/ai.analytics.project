import traceback
from pyspark import SparkContext
from collections import namedtuple
from pyspark.sql import SQLContext,Row
import datetime
#this process loads redshift data using Spark

#wget https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.2.1.1001.jar
#spark-submit --master local --packages com.databricks:spark-redshift_2.10:1.1.0 --jars /tmp/RedshiftJDBC41-1.2.1.1001.jar redshift-integration-process.py
#spark-submit --master yarn-client --driver-memory 8G --driver-cores 4 --num-executors 3 --executor-memory 8G --packages com.databricks:spark-redshift_2.10:1.1.0 --jars /tmp/RedshiftJDBC41-1.2.1.1001.jar redshift-integration-process.py

#COMMON
processTime = datetime.datetime.utcnow()
start_time= (processTime - datetime.timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
end_time=processTime.strftime('%Y-%m-%d %H:%M:%S')

fields=('count_click','received_revenue','offer_id')
aggregationDTO = namedtuple('aggregationDTO',fields)
###############################################################################################################
#M1
def M1(row):
	try:
		revenue = row['received_revenue']
		offerId = row['offer_id']
		return aggregationDTO(count_click=1,received_revenue=revenue,offer_id=offerId)
	#try
	except:
		traceback.print_exc()
	#except
#M1
###############################################################################################################

if __name__=='__main__':
	context = SparkContext(appName='Spark RedShift Connection')
	try:
		
		context._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAJ6GZUJPYDANGPCEQ")
		context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
		
		sqlContext = SQLContext(context)
		
		dataFrame = sqlContext.read.format('com.databricks.spark.redshift').option('url','jdbc:redshift://aitracker.cj1dejkknhi8.us-east-1.redshift.amazonaws.com:5439/aitracker?user=aitracker&password=AITrack123').option('query',"select received_revenue,offer_id from conversion_log where redshift_time >= '"+start_time +"' AND redshift_time <= '"+end_time+"' limit 1000").option('tempdir','s3a://tmp.redshiftlogs').load()
		
		inputRDD = dataFrame.rdd
		extractedRDD  = inputRDD.map(M1)		
		pairRDD =extractedRDD.map(lambda p : (p.offer_id,p))
		
		print 'size============'+ str(dataFrame.count())
		def R1(agg,o):
			try:
				c=agg.count_click+o.count_click
				r=o.received_revenue+agg.received_revenue
				return aggregationDTO(count_click=c,received_revenue=r,offer_id=agg.offer_id)
			#try
			except:
				traceback.print_exc()
			#except
		processedRDD = pairRDD.reduceByKey(R1)
		
		for tmp in processedRDD.collect():
			print "Data======================================>>>>>>>>>>>>>>"+str(tmp)
		
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'End of Process...'
	#finally
#if