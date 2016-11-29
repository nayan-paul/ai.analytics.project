import traceback
from pyspark import SparkContext
from collections import namedtuple
from pyspark.sql import SQLContext,Row
import datetime
#this process loads redshift data using Spark

#wget https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.2.1.1001.jar
#spark-submit --master local --packages com.databricks:spark-redshift_2.10:1.1.0 --jars /tmp/RedshiftJDBC41-1.2.1.1001.jar,/tmp/com.mysql.jdbc_5.1.5.jar redshift-integration-process.py
#spark-submit --master yarn-client --driver-memory 8G --driver-cores 4 --num-executors 3 --executor-memory 8G --packages com.databricks:spark-redshift_2.10:1.1.0 --jars /tmp/com.mysql.jdbc_5.1.5.jar,/tmp/RedshiftJDBC41-1.2.1.1001.jar redshift-integration-process.py

#*/5 * * * * sh /opt/projects/run_agg_redshift.sh
	

#COMMON
processTime = datetime.datetime.utcnow()
start_time= processTime.strftime('%Y-%m-%d')
end_time=processTime.strftime('%Y-%m-%d %H:%M:%S')

fields=('type','key','count_click','received_revenue','offer_id','paid_revenue','roi','affiliate_id','geo_country','subid4')
aggregationDTO = namedtuple('aggregationDTO',fields)

properties ={"driver": "com.mysql.jdbc.Driver"}
###############################################################################################################
#M1
def M1(row):
	try:
		revenue = row['received_revenue']
		offerId = row['offer_id']
		paidRevenue = row['paid_revenue']
		affiliateLst = []
		affiliateLst.append(str(row['affiliate_id']))
		geoLst = []
		geoLst.append(row['geo_country'])
		zoneLst=[]
		zoneLst.append(str(row['subid4']))
		return aggregationDTO(type='C',key=start_time,count_click=1,received_revenue=revenue,offer_id=offerId,paid_revenue=paidRevenue,roi=0,affiliate_id=affiliateLst,geo_country=geoLst,subid4=zoneLst)
	#try
	except:
		traceback.print_exc()
	#except
#M1
###############################################################################################################
#M2
def M2(o):
	try:
		k=o[0]
		v=o[1]
		r = v.received_revenue-v.paid_revenue
		affiliateSet = set(v.affiliate_id)
		geoSet=set(v.geo_country)
		zoneSet = set(v.subid4)
		return (v.key,v.type,str(v.offer_id),str(v.received_revenue),str(v.paid_revenue),str(r),str(v.count_click),"#".join(affiliateSet),"#".join(geoSet),"#".join(zoneSet))
		
	#try
	except:
		traceback.print_exc()
	#except
#M2
###############################################################################################################

if __name__=='__main__':
	context = SparkContext(appName='Spark RedShift Connection')
	sqlContext = SQLContext(context)
	try:
		
		context._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAJ6GZUJPYDANGPCEQ")
		context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
		
		dataFrame = sqlContext.read.format('com.databricks.spark.redshift').option('url','jdbc:redshift://aitracker.cj1dejkknhi8.us-east-1.redshift.amazonaws.com:5439/aitracker?user=aitracker&password=AITrack123').option('query',"select received_revenue,offer_id,paid_revenue,affiliate_id,geo_country,subid4 from conversion_log where conversion_time >= '"+start_time +"' AND conversion_time <= '"+end_time+"' ").option('tempdir','s3a://tmp.redshiftlogs').load()
		
		inputRDD = dataFrame.rdd
		extractedRDD  = inputRDD.map(M1)		
		pairRDD =extractedRDD.map(lambda p : (p.offer_id,p))
		
		print 'size============'+ str(dataFrame.count())
		def R1(agg,o):
			try:
				c=agg.count_click+o.count_click
				r=o.received_revenue+agg.received_revenue
				p=o.paid_revenue+agg.paid_revenue
				affiliateSet=filter(None,o.affiliate_id+agg.affiliate_id)
				geoSet=filter(None,o.geo_country+agg.geo_country)
				zoneSet = filter(None,o.subid4+agg.subid4)
				
				return aggregationDTO(type=o.type,key=agg.key,count_click=c,received_revenue=r,offer_id=agg.offer_id,paid_revenue=p,roi=0,affiliate_id=affiliateSet,geo_country=geoSet,subid4=zoneSet)
			#try
			except:
				traceback.print_exc()
			#except
		processedRDD = pairRDD.reduceByKey(R1)
		savedRDD = processedRDD.map(M2)
		processedDF=savedRDD.toDF(['key_dt','key_type','offerid','received_revenue','paid_revenue','roi','click_count','affiliates','geo','zones'])
		processedDF.write.jdbc(url='jdbc:mysql://aitracker.c3zkpgahaaif.us-east-1.rds.amazonaws.com:3306/aitracker?user=aitracker&password=aitracker',table='v2_agg_stats',mode='overwrite',properties=properties)
		
	#try
	except:
		traceback.print_exc()
	#except
	finally:
		context.stop()
		print 'End of Process...'
	#finally
#if