
from pyspark.sql.functions import *
from run_cmd import run_cmd

def saveToHive(df,table,db,spark,hivemode="new",partition=[]):

		""" staticmethod utilizado para cargar tablas a hive

			df: DataFrame a cargar
			table: Nombre de la tabla
			db: Nombre de la base de datos (si no existe, se crea)
			spark: Variable SparkSession
			hivemode: Por defecto new. La opcion es append para juntar varias tablas
			partition: """

		spark.sql("create database if not exists "+db)

		if hivemode=='new':
			df.write.format("parquet").partitionBy(partition).saveAsTable(db+"."+table)
		
		elif hivemode=='overwrite':
			run_cmd(["impala-shell","-q","drop table "+db+"."+table])
			df.write.format("parquet").partitionBy(partition).saveAsTable(db+"."+table)
		
		else:
			df.write.format("parquet").mode(hivemode).partitionBy(partition).saveAsTable(db+"."+table)
    		
		query="INVALIDATE METADATA "+db+"."+table
		run_cmd(["impala-shell","-q",query])