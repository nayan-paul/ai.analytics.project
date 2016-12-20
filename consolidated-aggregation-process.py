import traceback
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from collections import namedtuple
import sys
import datetime
#this process aggregates from v2_s3_stats and v2_agg_stats

#spark-submit --master local --packages com.databricks:spark-redshift_2.10:1.1.0 --jars /tmp/com.mysql.jdbc_5.1.5.jar,/tmp/RedshiftJDBC41-1.2.1.1001.jar  consolidated-aggregation-process.py 1
#spark-submit --master yarn-client --driver-memory 8G --driver-cores 4 --num-executors 3 --executor-memory 8G --jars /tmp/com.mysql.jdbc_5.1.5.jar /opt/projects/consolidated-aggregation-process.py 1

#*/7 * * * * sh /opt/projects/run_consolidated_s3agg_process.sh >> /opt/projects/run_consolidated_s3agg_process.stdout 2>>/opt/projects/run_consolidated_s3agg_process.stderr

#COMMON
properties ={"driver": "com.mysql.jdbc.Driver"}

currentTime = datetime.datetime.utcnow()
startTimeKey = currentTime.strftime('%Y-%m-%d')

s3_fields=('key_dt','key_type','offer_id','offer_advertiser_id','destination_subid','count_of_clicks')
s3_agg_dto = namedtuple('s3_agg_dto',s3_fields)
###############################################################################################################
#M1
def M1(row):
	try:
		key= row['key_dt']
		o = s3_agg_dto(key_dt=key,key_type=row['key_type'],offer_id=row['offer_id'],offer_advertiser_id=row['offer_advertiser_id'],destination_subid=row['destination_subid'],count_of_clicks=row['count_of_clicks'])
		return (str(key[:10])+row['offer_id'],o)
	#try
	except:
		print traceback.print_exc()
	#except
#M1
###############################################################################################################
#M2
def M2(o):
	try:
		v= o[1]
		return (v.key_dt[:10],v.key_type,str(v.offer_id),str(v.offer_advertiser_id),str(v.destination_subid),v.count_of_clicks)
	#try
	except:
		print traceback.print_exc()
	#except
#def
#M2
################################################################################################################
if __name__=='__main__':
	context = SparkContext(appName='Spark consolidated aggregation of redshift tables')
	sqlContext = SQLContext(context)
	try:
		if sys.argv[1]=='1':
			print "select key_dt,key_type,offer_id,offer_advertiser_id,destination_subid,count_of_clicks from v2_s3_stats where key_dt >= '"+startTimeKey +"' limit 300"
			dataFrame = sqlContext.read.format('jdbc').option('url','jdbc:mysql://aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com:3306/aitracker?user=aitracker&password=aitracker').option("driver", "com.mysql.jdbc.Driver").option('dbtable',"(select key_dt,key_type,offer_id,offer_advertiser_id,destination_subid,count_of_clicks from v2_s3_stats where key_dt >= '"+startTimeKey +"' limit 300)  as custom").load()
			
			inputRDD = dataFrame.rdd
			m1RDD = inputRDD.map(M1)
			
			def R1(agg,o):
				try:
					k1= datetime.datetime.strptime(agg.key_dt,'%Y-%m-%d %H:%M')
					k2= datetime.datetime.strptime(o.key_dt,'%Y-%m-%d %H:%M')
					
					if k1>=k2:
						return agg
					#if
					else:
						return o
					#else
				#try
				except:
					traceback.print_exc()
				#except
			#def
			r1RDD = m1RDD.reduceByKey(R1)
			
			m2RDD = r1RDD.map(M2)
			
			processedDF = m2RDD.toDF(['key_dt','key_type','offer_id','offer_advertiser_id','destination_subid','count_of_clicks'])
			processedDF.write.jdbc(url='jdbc:mysql://aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com:3306/aitracker?user=aitracker&password=aitracker',table='v2_s3_consolidate_stats',mode='append',properties=properties)
		#if
		elif sys.argv[1]=='2':
			print 'process 2'
		#if
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'end of process...'
	#finally
#if