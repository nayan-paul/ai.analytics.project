import traceback
from pyspark import SparkContext
from collections import namedtuple
from pyspark.sql import SQLContext,Row
#this process loads redshift data using Spark

#wget https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.2.1.1001.jar
#spark-submit --master local --packages com.databricks:spark-redshift_2.10:1.1.0 --jars /opt/projects/RedshiftJDBC41-1.2.1.1001.jar redshift-integration-process.py

if __name__=='__main__':
	context = SparkContext(appName='Spark RedShift Connection')
	try:
		context._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAJ6GZUJPYDANGPCEQ")
		context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
		
		sqlContext = SQLContext(context)
		
		dataFrame = sqlContext.read.format('com.databricks.spark.redshift').option('url','jdbc:redshift://aitracker.cj1dejkknhi8.us-east-1.redshift.amazonaws.com:5439/aitracker?user=aitracker&password=AITrack123').option('query','select * from conversion_log limit 10').option('tempdir','s3a://tmp.redshiftlogs').load()
		
		dataFrame.printSchema()
		
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'End of Process...'
	#finally
#if