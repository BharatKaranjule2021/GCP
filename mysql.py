#!/usr/bin/env python

import pymysql
from subprocess import Popen, PIPE, STDOUT

conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='root', db='demo')
cur = conn.cursor()

execution_date="2005-07-28"
sql ="select * from demo.payment where day(payment_date)={day} and month(payment_date)={month} and year(payment_date)={year} INTO OUTFILE 'E://Test_data//{report_date}demopayment.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'; ".format(day = execution_date[8:10], month = execution_date[5:7],year = execution_date[0:4],report_date = execution_date)
try:
    cur.execute(sql)
    print("Created dump daily records")
except Exception:
    print("Failed exporting daily database")
print("End of execution")