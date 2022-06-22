## SETUP INSTRUCTIONS 

# I already installed spark 3.1.2 version so there is no need to install it again 


# So in below i add the delta version 1.0.0 with my spark-shell 
spark-shell --packages io.delta:delta-core_2.12:1.0.0



# EXECUTION INSTRUCTIONS 

# i import the people.csv file from my localsystem 
val df=spark.read.format("csv").option("header","true").load("Downloads//people.csv")

df.show()

# here i convert the csv file in delta format and save it in localsystem 
df.write.format("delta").save("Downloads/delta-table")

# read the delta file into dataframe from my localsystem 
 val dfs = spark.read.format("delta").load("Downloads/delta-table")
 
  dfs.show()
  
  
  # count the records 
  
  dfs.count()
  dfs.distinct().count()
   dfs.columns
