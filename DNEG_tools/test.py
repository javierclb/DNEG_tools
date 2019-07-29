from Plexos import Plexos 
from savetohive import saveToHive
from pyspark.sql import SparkSession
from sys import argv
from getpass import getuser
import time 

t0=time.time()

base_path="hdfs://machicura.colbunsa.cl:8020/user/"
hive_path="hdfs:///user/hive/warehouse"
spark=SparkSession.builder.appName('Balinyret').master("yarn")\
                .enableHiveSupport().config("hive.metastore.warehouse.dir", hive_path)\
        .config("hive.exec.dynamic.partition", "true")\
        .config("spark.executor.memory","40g")\
        .config("spark.executor.memoryOverhead","40g")\
        .getOrCreate()

username=getuser()

path=base_path+"/"+username+"/"+argv[1]
filename=argv[2]
fecha=argv[3]
hivemode=argv[4]

plx=Plexos(path,filename, fecha,spark)
plx.genCSVs()
df=plx.get_t_data_0()

saveToHive(df,argv[5],"jdlf",spark,hivemode=hivemode)

t1=time.time()
print ("Elapsed Time: "+str(t1-t0)+"s")
