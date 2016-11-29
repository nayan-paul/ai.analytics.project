wget http://public-repo-1.hortonworks.com/HDP/tools/2.4.3.0/hdp_manual_install_rpm_helper_files-2.4.3.0.227.tar.gz
tar zxvf hdp_manual_install_rpm_helper_files-2.4.3.0.227.tar.gz

yarn-utils.py -c 6 -m 24 -d 2 -k False

http://54.152.197.111:7180/

admin/AIMedia*


create table v2_agg_stats(
key_dt varchar(20),
key_type varchar(2),
offer_id varchar(30),
received_revenue double,
paid_revenue double,
roi double,
click_count int,
affiliates varchar(500),
geo varchar(500),
zones varchar(500),
PRIMARY KEY(key_dt,key_type,offer_id))

create table v2_s3_stats(
key_dt varchar(20),
key_type varchar(20),
offer_id varchar(30),
offer_advertiser_id varchar(30),
destination_subid varchar(30),
count_of_clicks int,
PRIMARY KEY(key_dt,key_type,offer_id))