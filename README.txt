Ambari

http://54.152.197.111:7180/

admin/AImedia1234*

From SPARK_HOME, start the Spark SQL Thrift Server. Specify the port value of the Thrift Server (the default is 10015). For example:

su spark

./sbin/start-thriftserver.sh --master yarn-client --executor-memory 512m --hiveconf hive.server2.thrift.port=100015
