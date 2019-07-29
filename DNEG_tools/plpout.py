"""clase para tratamiento de salidas PLP. 
Ejemplo de inicializacion out=plpout(spark,"PxP","c00","201906",empresa="Colbun",ord_hid=False,horario=False)
La clase requiere que los archivos esten almacenados en HDFS"""

from pyspark.sql import SparkSession
from sys import argv, exit
import time
from pyspark.sql.functions import *
import pandas as pd
import subprocess
from run_cmd import run_cmd


class plpout:
	"""Clase para tratamiento de salidas PLP"""
	def  __init__(self,spark,base,case_std,date,empresa="Colbun",ord_hid=False,ver="GGNE",horario=False,sep="-",**kwargs):

		self.base=base
		self.spark=spark
		self.case_std=case_std
		self.date=date
		self.ruta="bases/"+empresa
		self.ord_hid=ord_hid
		self.path=base+"/"+date+"/"+case_std
		self.ver=ver
		self.horario=horario
		self.sep=sep
		self.etapas_df=self.etapas()
		self.indhor_df=self.indhor()

	def clientes(self,filename=None):
		"""Cargar datos de clientes por contrato.
 		Si no se hace referencia a un archivo, 
		carga proyeccion de ventas desde base de datos Clientes almacenada en Hive
		Archivo debe ubicarse en carpeta Base en HDFS """ 
		ruta=self.ruta
		spark=self.spark

		if filename != None:

			#importar archivos de base
			clientes=spark.read.format("csv")\
					   .options(header=True,inferSchema=True)\
					   .load(ruta+'/'+filename+'.csv')

			#Limpieza de data de clientes
			clientes=clientes.withColumn("Fecha",(unix_timestamp("Fecha","yyyy-MM-dd")+(col("Hora")-1)*3600).cast("timestamp"))\
				 	.selectExpr("Fecha as fecha","Retiros as fisico","cliente","barra_1 as barnom","contrato as descripcion")\
				 	.withColumn("barnom",lower(rtrim(ltrim(col("barnom")))))
		
		else:
			 clientes=spark.sql("select * from bases_clientes.retiros_proy")
		
		return clientes		   



	def to_df(self,filename):
		"""Convierte archivos csv en directorio definido en inicializador de clase, a DataFrame de Spark"""
		path=self.path
		spark=self.spark
		df=spark.read.format("csv")\
			.options(header=True,inferSchema=True)\
			.load(path+"/"+filename +".csv")
		return df

	def etapas(self):
		sep=self.sep
 		etapas_df=self.to_df("etapas")\
        		      .withColumnRenamed("#Bloque","Bloque")\
			      .withColumnRenamed(" Categoria","Categoria")\
			      .withColumn("Fecha",(unix_timestamp(" Fecha","dd"+sep+"MM"+sep+"yyyy").cast("timestamp")))\
			      .drop(" Fecha")\
			      .withColumnRenamed(" Tipo","Tipo")\
			      .withColumnRenamed(" Duracion","Duracion")
				
		return etapas_df

	def indhor(self):
		indhor_df=self.to_df("indhor")\
			      .withColumn("fecha", concat_ws("-", "Dia", "Mes", "Anio"))\
			      .withColumn("fecha",(unix_timestamp("fecha","dd-MM-yyyy")+(col("Hora")-1)*3600).cast("timestamp"))
		return indhor_df

	def ordhid(self):
		ordhid_df=self.to_df("ordhid")\
			      .withColumn("fecha", unix_timestamp("Fecha","dd-MM-yyyy").cast("timestamp"))\
			      .withColumn("Sim", col("Sim").cast('string'))\
			      .selectExpr("fecha as mes","Sim as hidro","Index")				
		return ordhid_df

    	def plpbar(self):
    		"""Crea DataFrame con salida PLPBAR, ya limpia"""	
    		plpbar_df=self.to_df("plpbar")
    		#df_ordhid=self.ordhid()
    		etapas_df=self.etapas_df
    		ord_hid=self.ord_hid
    		date=self.date
		case_std=self.case_std	
		plpbars=plpbar_df.join(etapas_df,["Bloque"],how="left_outer")\
				 .selectExpr("cmgbar","Bloque","Hidro","barnom","barnum as num","Fecha","Categoria","Duracion","Tipo")\
				 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
				 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
			         .withColumn("caso",lit(date+"_"+case_std))\
			         .withColumn("mes",date_trunc("month","fecha"))
		if ord_hid:			     
			plpbar_final=plpbars.join(self.df_ordhid,(["hidro","mes"])).drop("mes","hidro")
		else:
			plpbar_final=plpbars.withColumnRenamed("Hidro","Index").drop("mes")	
		return plpbar_final		  
	
	def plp_bar_cen(self):
    		"""Crea DataFrame con salida PLPCEN y PLPBAR ya limpia"""
    		#df_ordhid=self.ordhid()
    		ord_hid=self.ord_hid
    		etapas_df=self.etapas_df
		plpcen_df=self.to_df("plpcen")
		date=self.date
		case_std=self.case_std
		plpcens=plpcen_df.join(etapas_df,["Bloque"])\
			         .selectExpr("cenegen as fisico","Bloque","Hidro","Tipo","cennom","ceninye as valorizado","fecha","barnom","cencvar","Categoria","Duracion")\
				 .withColumn("cennom",ltrim(rtrim(col("cennom"))))\
				 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
				 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
			         .withColumn("caso",lit(date+"_"+case_std))\
			         .withColumn("mes",date_trunc("month","fecha"))
		
		if ord_hid:			     
			plpcen_final=plpcens.join(df_ordhid,(["hidro","mes"])).drop("mes","hidro")
		else:
			plpcen_final=plpcens.withColumnRenamed("Hidro","Index").drop("mes")
            
        	plpbar_final=self.plpbar()    
        
        	plpcen_final=plpbar_final.join(plpcen_final,["Bloque","Tipo","caso","barnom","Index","fecha","Categoria","Duracion"])
		
		plpcen_final=plpcen_final.selectExpr("caso","fecha","cennom","Tipo","barnom","bloque","Index","fisico","categoria","fisico*cmgbar as val")
		
		return plpbar_final,plpcen_final		  


	def plplin(self):
		plplin_df=self.to_df("plplin")
		etapas_df=self.etapas_df
		date=self.date
		case_std=self.case_std		

		plplin_df=plplin_df.selectExpr("Hidro","BarA","BarB","Bloque","LinNom","LinFluP","LinFluMax","LinUso")
		plplin_df=plplin_df.join(etapas_df,"Bloque")\
        		.selectExpr("Hidro","stack(2, 'A', BarA, 'B', BarB) as (barra, num)","Bloque","LinNom","LinFluP","LinFluMax","LinUso","categoria","fecha","Tipo")\
        		.withColumn("Index",lower(ltrim(rtrim(col("hidro"))))).drop("hidro")\
			.withColumn("caso",lit(date+"_"+case_std))
		
		return plplin_df
        
	def plpbar_hor(self):
                #df_ordhid=self.ordhid()
                ord_hid=self.ord_hid
                indhor_df=self.indhor_df
                plpbar_df=self.to_df("plpbar")
                date=self.date
                case_std=self.case_std

                plpbars=plpbar_df.join(indhor_df,["Bloque"])\
                                 .selectExpr("cmgbar","Hidro","fecha","barnom")\
                                 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
                                 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
                                 .withColumn("caso",lit(date+"_"+case_std))\
                                 .withColumn("mes",date_trunc("month","fecha"))

                if ord_hid:
                        plpbar_final=plpbars.join(df_ordhid,(["hidro","mes"])).drop("mes","hidro")
                else:
                        plpbar_final=plpbars.withColumnRenamed("Hidro","Index").drop("mes")
		
		return plpbar_final

	def plpcen_hor(self):
		#df_ordhid=self.ordhid()
                ord_hid=self.ord_hid
                indhor_df=self.indhor_df
                plpcen_df=self.to_df("plpcen")
                date=self.date
                case_std=self.case_std

                plpcens=plpcen_df.join(indhor_df,"Bloque")\
                                 .selectExpr("cenpgen as fisico","Hidro","cennom","ceninyp as valorizado","fecha","barnom","cencvar")\
                                 .withColumn("cennom",ltrim(rtrim(col("cennom"))))\
                                 .withColumn("barnom",lower(ltrim(rtrim(col("barnom")))))\
                                 .withColumn("hidro",lower(ltrim(rtrim(col("hidro")))))\
                                 .withColumn("caso",lit(date+"_"+case_std))\
                                 .withColumn("mes",date_trunc("month","fecha"))

                if ord_hid:
                        plpcen_final=plpcens.join(df_ordhid,(["hidro","mes"])).drop("mes","hidro")
                else:
                        plpcen_final=plpcens.withColumnRenamed("Hidro","Index").drop("mes")

                return plpcen_final

	def plp_one_liner(self):
		"""Metodo rapido que devuelve dataframes de plpbar y plpcen"""
		
		if self.horario:
			plpbar_df=self.plpbar_hor()
			plpcen_df=self.plpcen_hor() 
		
		else:
			plpbar_df=self.plpbar().groupby("Index","barnom","fecha","categoria","caso")\
				       	       .agg((sum(col("duracion")*col("cmgbar"))/sum("duracion")).alias("CMg"))

			plpcen_df=self.plpcen().groupby("Index","barnom","fecha","categoria","caso","cennom")\
				       	       .agg(avg("cencvar").alias("cvar"),sum("fisico").alias("fisico"),sum("valorizado").alias("valorizado"))
		return plpbar_df,plpcen_df


	def centrales(self):
		ver=self.ver
		ruta=self.ruta
		spark=self.spark
		if ver=="GGNE":
			centrales_df=spark.read.format("csv")\
				       .options(header=True,inferSchema=True)\
				       .load(ruta+'/cennom_ggne.csv')\
				       .withColumn("cennom",ltrim(rtrim(col("cennom"))))
		else:
			centrales_df=spark.read.format("csv")\
				       .options(header=True,inferSchema=True)\
				       .load(ruta+'/cennom.csv')\
				       .withColumn("cennom",ltrim(rtrim(col("cennom"))))
		return centrales_df

	def iny_ret(self,filename):
		""" Metodo que calcula Balance de inyecciones y retiros.
			Devuelve una tupla de la forma balance, plpbar, plpcen"""
		
		clientes_df=self.clientes(filename=filename)
		plpbar_final,plpcen_final=self.plp_bar_cen()
		centrales=self.centrales()
		indhor_df=self.indhor_df
		etapas_df=self.etapas_df
		date=self.date
		case_std=self.case_std
		
		cl=clientes_df.join(indhor_df,"fecha")\
			      .drop("fecha","Anio","Hora","Dia")\
		              .join(etapas_df,"Bloque")\
			      .groupby("Bloque","descripcion","cliente","barnom")\
			      .agg((sum("fisico")/1000).alias("fisico"))
		# Retiros Valorizados
		ret_val=plpbar_final.join(cl,["barnom","Bloque"]).withColumn("valorizado",-col("fisico")*col("cmgbar"))\
				    .selectExpr("fecha","descripcion","fisico","valorizado","Index","barnom as barra","Bloque","Categoria")\
				    .withColumn("tipo",lit("ret"))

		#Fecha limite
		max_date=ret_val.agg({"fecha": "max"}).collect()[0]["max(fecha)"]

		#Inyeccion valorizada    
		iny_val=plpcen_final.join(centrales,["cennom"])\
				    .selectExpr("fecha","Central as descripcion","fisico","val as valorizado","Index","barnom as barra","Bloque","Categoria")\
				    .withColumn("tipo",lit("iny"))

		#Filtro de fecha
		iny_val=iny_val.filter(iny_val.fecha<=max_date)

		#Balance Inyecciones y retiros
		balance=iny_val.unionAll(ret_val).withColumn("caso",lit(date+"_"+case_std))
	    
		return balance,plpbar_final,plpcen_final
	
	def info_barras(self):
		spark=self.spark
		geo=spark.read.format("csv")\
			.options(header=True,inferSchema=True)\
			.load("PLP/Barras.csv")\
			.withColumn("barnom",lower(ltrim(rtrim(col("Barra")))))\
			.drop("Barra")
		return geo 


	def delete_hdfs(self):
		"""Borra archivos de la corrida de HDFS"""
		path=self.path
		run_cmd(["hadoop","fs","-rm","-r","-f",path])