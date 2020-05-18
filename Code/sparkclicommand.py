file_path1 = "/data/weather/2010"
file_path2 = "/data/weather/2011"
file_path3 = "/data/weather/2012"
file_path4 = "/data/weather/2013"
file_path5 = "/data/weather/2014"
file_path6 = "/data/weather/2015"
file_path7 = "/data/weather/2016"
file_path8 = "/data/weather/2017"
file_path9 = "/data/weather/2018"
file_path10 = "/data/weather/2019"
inTextData1 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path1)

def dropColumns(x):
    columns = x.split(" ")
    output = columns[0] + " " + columns[2] + " " + columns[3] + " " + columns[5] + " " + columns[7] + " " + columns[
        9] + " " + columns[11] + " " + columns[13] + " " + columns[15] + " " + columns[16] + " " + columns[17] + " " + \
             columns[18] + " " + columns[19] + " " + columns[20] + " " + columns[21]
    return output

def cleaningdata(x,name):
    from pyspark.sql.functions import col, split
    rdd1 = x.rdd
    rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
    rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
    rdd4 = rdd3.map(lambda x: x[1:-2])
    rdd5 = rdd4.map(lambda x: x.replace("'", ""))
    rdd6 = rdd5.map(dropColumns)
    file_store = "/user/balakrba/"+name
    rdd6.saveAsTextFile(file_store)
    newInData = spark.read.csv(file_store , header=False, sep=' ')
    cleanData1 = newInData.withColumnRenamed('_c0', 'STN').withColumnRenamed('_c1', 'YEARMODA') \
        .withColumnRenamed('_c2', 'TEMP').withColumnRenamed('_c3', 'DEWP') \
        .withColumnRenamed('_c4', 'SLP').withColumnRenamed('_c5', 'STP') \
        .withColumnRenamed('_c6', 'VISIB').withColumnRenamed('_c7', 'WDSP') \
        .withColumnRenamed('_c8', 'MXSPD').withColumnRenamed('_c9', 'GUST') \
        .withColumnRenamed('_c10', 'MAX').withColumnRenamed('_c11', 'MIN') \
        .withColumnRenamed('_c12', 'PRCP').withColumnRenamed('_c13', 'SNDP') \
        .withColumnRenamed('_c14', 'FRSHTT')
    cleanData2 = cleanData1.withColumn("MIN", split(col("MIN"), "\\*").getItem(0))
    cleanData3 = cleanData2.withColumn("MAX", split(col("MAX"), "\\*").getItem(0))
    cleanData3 = cleanData3.withColumn("MAX_NEW", cleanData3["MAX"].cast("double"))
    cleanData3 = cleanData3.withColumn("MIN_NEW", cleanData3["MIN"].cast("double"))
    cleanData3 = cleanData3.drop('MAX', 'MIN')
    cleanData3 = cleanData3.withColumnRenamed("MAX_NEW", "MAX")
    cleanData3 = cleanData3.withColumnRenamed("MIN_NEW", "MIN")
    cleanData4 = cleanData3.filter(cleanData3.MAX != 9999.9)
    cleanData4 = cleanData4.filter(cleanData4.MIN != 9999.9)
    return cleanData4
	

inTextData2 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path2)
inTextData3 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path3)
inTextData4 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path4)
inTextData5 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path5)
inTextData6 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path6)
inTextData7 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path7)
inTextData8 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path8)
inTextData9 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path9)
inTextData10 = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(file_path10)

inTextData = inTextData1.union(inTextData2).union(inTextData3).union(inTextData4).union(inTextData5).union(inTextData6).union(inTextData7).union(inTextData8).union(inTextData9).union(inTextData10).distinct()
cleaned = cleaningdata(inTextData,"part10_19")	
cleanData = cleaned.withColumn("MAX_NEW", cleanData["MAX"].cast("double"))
cleanData = cleanData.withColumn("MIN_NEW", cleanData["MIN"].cast("double"))
cleanData = cleaned.withColumn("PRCP_NEW", cleanData["PRCP"].cast("double"))
cleanData = cleanData.withColumn("GUST_NEW", cleanData["GUST"].cast("double"))
cleanData = cleanData.drop('MAX', 'MIN', 'PRCP', GUST)
cleanData = cleanData.withColumnRenamed("MAX_NEW", "MAX")
cleanData = cleanData.withColumnRenamed("MIN_NEW", "MIN")
cleanData = cleanData.withColumnRenamed("PRCP_NEW", "PRCP")
cleanData = cleanData.withColumnRenamed("GUST_NEW", "GUST")
from pyspark.sql.functions import col, split
cleanData.createOrReplaceTempView("Data_All")

missingdf=cleanData.where("STP='9999.9'")
missingtotal=(float)(missingdf.count())
total=(float)(cleanData.count())
percent=(missingtotal*100)/(total)
print(percent)

