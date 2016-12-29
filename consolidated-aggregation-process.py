import traceback
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from collections import namedtuple
import sys
import datetime
import pymysql
#this process aggregates from v2_s3_stats and v2_agg_stats

#chmod -R 755 /home
#chmod -R 775 /opt/anaconda
#export PYSPARK_PYTHON=/root/anaconda2/bin/python
#PYSPARK_PYTHON=/opt/anaconda/bin/python spark-submit --master local --jars /tmp/com.mysql.jdbc_5.1.5.jar consolidated-aggregation-process.py 1
#PYSPARK_PYTHON=/opt/anaconda/bin/python spark-submit --master yarn-client --driver-memory 8G --driver-cores 4 --num-executors 3 --executor-memory 8G --jars /tmp/com.mysql.jdbc_5.1.5.jar /opt/projects/consolidated-aggregation-process.py 1

#PYSPARK_PYTHON=/opt/anaconda/bin/python spark-submit --master local --jars /tmp/com.mysql.jdbc_5.1.5.jar consolidated-aggregation-process.py 2

#crontab -e
#*/45 * * * * sh /opt/projects/run_consolidated_s3agg_process.sh >> /opt/projects/run_consolidated_s3agg_process.stdout 2>>/opt/projects/run_consolidated_s3agg_process.stderr

#COMMON
properties ={"driver": "com.mysql.jdbc.Driver"}

currentTime = datetime.datetime.utcnow() 
startTimeKey = currentTime.strftime('%Y-%m-%d')

s3_fields=('key_dt','key_type','offer_id','offer_advertiser_id','destination_subid','count_of_clicks')
s3_agg_dto = namedtuple('s3_agg_dto',s3_fields)

agg_fields = ('key_dt','key_type','received_revenue','paid_revenue','roi','click_count')
agg_dto=namedtuple('agg_dto',agg_fields)
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
#M3
def M3(row):
	try:
		key = row['key_dt'][:10]
		o= agg_dto(key_dt=key,key_type=row['key_type'],received_revenue=row['received_revenue'],paid_revenue=row['paid_revenue'],roi=row['roi'],click_count=row['click_count'])
		return (key+"-"+o.key_type,o)
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process'
	#finally
###################################################################################################
#M4
def M4(o):
	try:
		v= o[1]
		return (v.key_dt,v.key_type,v.received_revenue,v.paid_revenue,v.roi,v.count_of_clicks)
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		print 'end of process'
	#finally
###################################################################################################
if __name__=='__main__':
	context = SparkContext(appName='Spark consolidated aggregation of redshift tables')
	sqlContext = SQLContext(context)
	try:
		if sys.argv[1]=='1':
			dataFrame = sqlContext.read.format('jdbc').option('url','jdbc:mysql://aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com:3306/aitracker?user=aitracker&password=aitracker').option("driver", "com.mysql.jdbc.Driver").option('dbtable',"(select key_dt,key_type,offer_id,offer_advertiser_id,destination_subid,count_of_clicks from v2_s3_stats where key_dt >= '"+startTimeKey +"' )  as custom").load()
			
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
			
			con  = pymysql.connect(user='aitracker',password='aitracker',host='aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com',database='aitracker' ,port=3306)
			cursor = con.cursor()
			for o in m2RDD.collect():
				cursor.execute("INSERT INTO v2_s3_consolidate_stats VALUES('"+str(o[0])+"','"+str(o[1])+"','"+str(o[2])+"','"+str(o[3])+"','"+str(o[4])+"',"+str(o[5])+") ON DUPLICATE KEY UPDATE count_of_clicks="+str(o[5])+"")
			#for
			cursor.close()
			con.commit()
			con.close()
			#processedDF = m2RDD.toDF(['key_dt','key_type','offer_id','offer_advertiser_id','destination_subid','count_of_clicks'])	#processedDF.write.jdbc(url='jdbc:mysql://aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com:3306/aitracker?user=aitracker&password=aitracker',table='v2_s3_consolidate_stats',mode='append',properties=properties)
		#if
		elif sys.argv[1]=='2':
			print 'process 2'
			print "(select key_dt,key_type,received_revenue,paid_revenue,roi,click_count from v2_agg_stats where key_dt >= '"+startTimeKey +"' ) "
			dataFrame = sqlContext.read.format('jdbc').option('url','jdbc:mysql://aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com:3306/aitracker?user=aitracker&password=aitracker').option("driver", "com.mysql.jdbc.Driver").option('dbtable',"(select key_dt,key_type,received_revenue,paid_revenue,roi,click_count from v2_agg_stats where key_dt >= '"+startTimeKey +"' )  as custom").load()
			
			input = dataFrame.rdd
			m3RDD = input.map(M3)
			print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
			print m3RDD.first()
			def R2(agg,o):
				try:
					agg.received_revenue = agg.received_revenue+o.received_revenue
					agg.paid_revenue=agg.paid_revenue+o.paid_revenue
					agg.roi = agg.roi+o.roi
					agg.count_of_clicks = agg.count_of_clicks+o.count_of_clicks
					
					return agg
				#try
				except:
					traceback.print_exc()
				#except
			r2RDD = m3RDD.reduceByKey(R2)
			m4RDD = r2RDD.map(M4)
			
			for o in m4RDD.collect():
				print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'+o
			#for
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