import sys, subprocess, os
from pyspark import SparkFiles
from pyspark import SparkContext
from run_cmd import run_cmd
from os import remove, getcwd, listdir
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
from Embalses import Embalses
from savetohive import saveToHive


usedFiles=['t_data_0','t_key','t_period_0','t_membership',\
			't_property','t_unit','t_collection','t_object',\
				't_category','t_model','t_class']

propiedad_list = ["Flow","Generation","Price","Cota","Export Limit","Import Limit"]
clase_list = ["Generators","Lines","Nodes","Storages"]


class Plexos:
	def __init__(self, path,accbd_file,fecha,spark, usedFiles=usedFiles,date_format="MM/dd/yy HH:mm"):
		self.path = path
		self.accbd_file = accbd_file
		self.usedFiles = usedFiles
		self.spark=spark
		self.date_format=date_format
		self.fecha=fecha
		self.sc=spark.sparkContext

	def genCSVs(self):
		sc=self.sc
		path=self.path
		pdata = sc.addFile(path+'/'+self.accbd_file)
		data = SparkFiles.get(self.accbd_file)
		table_names = subprocess.Popen(["mdb-tables","-1",data], stdout = subprocess.PIPE).communicate()[0]
		#print table_names
		tables = table_names.splitlines()
		print tables
		
		for table in tables:
			if table in self.usedFiles:
				file = open(table+'.csv', 'w')
				contents = subprocess.Popen(["mdb-export", data, table],
											stdout = subprocess.PIPE).communicate()[0]
				file.write(contents)
				file.close()
				
				(ret,out,err) = run_cmd(['hdfs','dfs','-put','-f',table+'.csv',path])
				remove(getcwd()+"/"+table+".csv")

	def to_df(self,filename):
		"""Convierte archivos csv en directorio definido en inicializador de clase, a DataFrame de Spark"""
		path=self.path
		spark=self.spark
		df=spark.read.format("csv")\
			.options(header=True,inferSchema=True)\
			.load(path+"/"+filename +".csv")
		return df

	def load_df(self):
		dfs={}
		for File in self.usedFiles:
			print("Loading "+File)
			dfs[File]=self.to_df(File)
		return dfs


	def get_t_data_0(self,propiedad_list=propiedad_list,clase_list=clase_list):
		sc=self.sc
		sc.addPyFile("Embalses.py")
		df=self.load_df()

		@udf("double")
		def cota(nombre,volumen):
			emb = Embalses()
			try:
				cota = emb.cota(nombre,volumen*0.0864)
			except:
				cota = 0
			return cota

		df["t_period_0"]=df["t_period_0"]\
			    .withColumn("datetime",(unix_timestamp("DATETIME",self.date_format)-4*3600)\
				.cast("timestamp"))

		df["t_parent"]=df["t_object"]
		df["t_child"]=df["t_object"]
		
		del df["t_object"]

		t_data_0=df["t_data_0"]
		
		t_data_0 = t_data_0.join(df["t_key"],"key_id")
		t_data_0 = t_data_0.join(df["t_period_0"]\
		                   .selectExpr("interval_id as period_id","datetime"),"period_id")
		
		t_data_0 = t_data_0.join(df["t_membership"],"membership_id")
		
		t_data_0 = t_data_0.join(df["t_property"]\
		                   .selectExpr("property_id","name as propiedad","unit_id","lang_id"),"property_id")
		
		t_data_0 = t_data_0.join(df["t_collection"]\
		                   .selectExpr("collection_id","name as objeto"),"collection_id")
		
		t_data_0 = t_data_0.join(df["t_parent"]\
			               .selectExpr("class_id","name as nombre1","category_id","index","object_id as parent_object_id"),"parent_object_id")
		
		t_data_0 = t_data_0.join(df["t_child"]\
			               .selectExpr("name as nombre2","category_id","index","object_id as child_object_id"),"child_object_id")

		t_data_0 = t_data_0.join(df["t_class"]\
			               .select("class_id","name"),"class_id")

		t_data_0 = t_data_0.drop("category_id","index","key_id","period_id","membership_id","model_id","phase_id",\
			               "property_id","period_type_id","band_id","sample_id","timeslice_id","parent_class_id",\
							   "child_class_id","collection_id","parent_object_id","child_object_id","unit_id","lang_id",\
								   "class_id","nombre1","name")
		
		t_data_0 = t_data_0.withColumnRenamed("nombre2","Nombre")
		t_data_0 = t_data_0.withColumnRenamed("objeto","Clase")

		t_cota = t_data_0.filter((t_data_0.Clase=="Storages") & (t_data_0.propiedad=="End Volume"))\
			           .withColumn("Value",cota(col("Nombre"),col("Value")))\
	                   .withColumn("Propiedad",lit("Cota"))
		
		t_data_0 = t_data_0.unionAll(t_cota)

		t_data_0 = t_data_0.withColumn("FechaPrograma",unix_timestamp(lit(self.fecha),'yyyy-MM-dd')\
			               .cast("timestamp"))
		
		t_data_0 = t_data_0.where(col("Propiedad").isin(propiedad_list))\
                           .where(col("Clase").isin(clase_list))
		return t_data_0

	def Plexos_full(self,propiedad_list=propiedad_list,clase_list=clase_list):
		self.genCSVs()
		BDsalida=self.get_t_data_0(propiedad_list=propiedad_list,clase_list=clase_list)

		return BDsalida
