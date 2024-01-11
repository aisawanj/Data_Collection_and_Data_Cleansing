
# **Data Collection (Pandas) and Data Cleansing (Spark)**

Getting data from the MySQL database, Rest API and Google Drive using PyMySQL and Requests packages by integrating data using Pandas to create a global schema. Subsequently, data cleansing is performed with PySpark, and processed data is upload to a data lake on Google Cloud Storage with gcloud SDK

Reference on google colab:  [Click Here](https://colab.research.google.com/github/aisawanj/Data_Collection_and_Data_Cleansing/blob/main/Project_01_Data_Collection_with_Pandas_and_Data_Cleansing_with_Spark.ipynb)

Remarks: The Source Code has been referenced URL from DataTH for testing "project_01" / Cr. DataTH.


## **Section 1: Data Collection with Pandas**

- Create a variable to save passwords in the env file for use connect to the database

```%%writefile .env```

### 1.1) Connect to Database

```bash
# Install pymysql for connect to MySQL database
!pip install pymysql

# Install python-dotenv for read variables from .env file
!pip install python-dotenv

# Browse and read variables from .env
import os
from dotenv import load_dotenv
load_dotenv()

class config:
  MYSQL_HOST = os.getenv("MYSQL_HOST")
  MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
  MYSQL_USER = os.getenv("MYSQL_USER")
  MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
  MYSQL_DB = os.getenv("MYSQL_DB")
  MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")

# Connect to the database
import pymysql
connection = pymysql.connect(host = config.MYSQL_HOST,
                             port = config.MYSQL_PORT,
                             user = config.MYSQL_USER,
                             password = config.MYSQL_PASSWORD,
                             db = config.MYSQL_DB,
                             charset = config.MYSQL_CHARSET,
                             cursorclass = pymysql.cursors.DictCursor)

# Connect to Google Drive database for import data
from google.colab import drive
drive.mount('/content/drive')

```

### 1.2) Get data from the database

```bash
import pandas as pd

# Query data from Database
sql = "SELECT * FROM audible_data"
audible_data = pd.read_sql(sql, connection)
audible_data = audible_data.set_index("Book_ID")

# Import data from Google Drive into Colab
path = "/content/drive/MyDrive/My data/audible_transaction.csv"
audible_transaction = pd.read_csv(path)

# Get data from REST API
import requests
url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
r = requests.get(url)
result_conversion_rate = r.json()

# Create new datafrme "conversion_rate"
conversion_rate = pd.DataFrame(result_conversion_rate)

# Change the index to the column "date" and save to CSV file "conversion_rate.csv"
conversion_rate = conversion_rate.reset_index().rename(columns={"index":"date"})
```

### 1.3) Dataframe overview before integrating data

audible_data

```bash
# Review dataframe "audible_data"
audible_data.head()
```
![image.png](https://drive.google.com/uc?export=download&id=1h3R6DY48cpsZbRrDeqY9Pwg2ylGeX9QR)

audible_transaction

```bash
# Review dataframe "audible_transaction"
audible_transaction.head()
```
![image.png](https://drive.google.com/uc?export=download&id=1xLcf7igS9CeaspOJ7wn2Q7k4b4UbNsRs)

conversion_rate

```bash
# Review dataframe "conversion_rate"
conversion_rate.head()
```
![image.png](https://drive.google.com/uc?export=download&id=1EVm1htkpzBfUgO8pEqvc5rAwnymfJVqZ)


### 1.4) Data Integration

```bash
# Create Table "transaction" by merge table "audible_data" & "audible_transaction"
transaction = pd.merge(audible_transaction, audible_data, how="left", left_on="book_id", right_on="Book_ID")

# Convert data type to be date type in both dataframes (transaction, conversion_rate)
transaction["date"] = transaction["timestamp"]
transaction["date"] = pd.to_datetime(transaction["timestamp"]).dt.date
conversion_rate["date"] = pd.to_datetime(conversion_rate["date"]).dt.date

# Create new dataframe "final_df" by merge 2 dataFrame (transaction & conversion_rate)
final_df = pd.merge(transaction, conversion_rate, how="left", left_on="date", right_on="date")

# Edit column "Price" by replacing "$" to "" and converting data type is float
final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$", ""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)

# Create a new column "THB_Price" with column "Price" multiply "conversion_rate
final_df["THB_Price"] = final_df["Price"]
final_df["THB_Price"] = final_df.apply(lambda x: x["Price"] * x["conversion_rate"], axis=1)

# Rename column "Total No. of Ratings" to "Total_No_of_Ratings"
final_df = final_df.rename(columns={"Total No. of Ratings" : "Total_No_of_Ratings"})

# Drop column "date"
final_df =  final_df.drop("date", axis=1)

# Save to CSV file
final_df.to_csv("transaction_data.csv", index=False)

```

### 1.5) Dataframe overview after integrating data

```bash
final_df.head()
```
![image.png](https://drive.google.com/uc?export=download&id=1n3YchcrXGS10urjYjdg1TvpEGau9n5CU)


## **Section 2: Data Cleansing with Spark**

### 2.1) Install Spark and Pyspark

```bash
# Update the total Package in VM
!apt-get update     
# Install Java Development Kit (necessary for install Spark)
!apt-get install openjdk-8-jdk-headless -qq > /dev/null       
# Install Spark 3.1.2
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz  
# Unzip file Spark 3.1.2
!tar xzvf spark-3.1.2-bin-hadoop2.7.tgz                                                 
# Install Package Python for connect Spark
!pip install -q findspark==1.3.0                                                   

# Set environment variable for Python to get to know spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"

# Install PySpark into Python
!pip install pyspark==3.1.2

# Create Spark Session for Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
```

### 2.2) Data Profiling

Import data from section 1

```bash
# Read csv file
dt = spark.read.csv('/content/transaction_data.csv', header = True, inferSchema = True)
```

Data overview

```bash
# Check the data overview
print(f"Rows: {dt.count()}, Columns: {len(dt.columns)}\n")
dt.show(10, False)
dt.printSchema()
dt.summary("count").show()
dt.summary().show()
```
![image.png](https://drive.google.com/uc?export=download&id=1vBOrETP6jVvJJzhVFLhkNDmlvlscKL0E)

### 2.3) EDA : Exploratory Data Analysis

Graphical EDA

```bash
# Convert Spark dataframe to Pandas dataframe
import pandas as pd
dt_pd = dt.toPandas()

# Data visualization with package "seaborn"
import seaborn as sns
sns.histplot(dt_pd["Price"], bins=10)
```
![image.png](https://drive.google.com/uc?export=download&id=1q0xa0OERraRQDZTGss_CMhxwlgFk_zDY)

Interactive Chart with plotly package

```bash
# Data visualization with plotly (Interactive Chart)
import plotly.express as px
fig = px.scatter(dt_pd, "book_id", "Price")
fig.show()
```
![image.png](https://drive.google.com/uc?export=download&id=1Krp_tWnuotE-_CjCi7_xdtls8b2PXTEV)

### 2.4) Convert data type

Before converting the data type

```bash
dt.printSchema()
```
![image.png](https://drive.google.com/uc?export=download&id=1HjejO3mbjpbQoXB5Dx0IQC7JOEY8A1r-)

Convert data type

```bash
# Convert column "timestamp" from string to timestamp data type
from pyspark.sql import functions as f
dt_clean = dt.withColumn("timestamp", f.to_timestamp(dt["timestamp"], "yyyy-MM-dd HH:mm:ss"))
```

```bash
dt_clean.printSchema()
```
![image.png](https://drive.google.com/uc?export=download&id=1UjNMop4lugGXfylaarGd51rfnQHDvPjI)

### 2.5) Data Anomaly

(2.5.1) Syntactical Anomalies

- Check column "country"

```bash
# Check column "country"
country_count = dt_clean.select(dt_clean["country"]).distinct().sort("country").count()
print(f"country count = {country_count}\n")
dt_clean.select(dt_clean["country"]).distinct().sort("country").show(59, False)
```
- Detected data anomalies: "Japane" and "Philippine"

![image.png](https://drive.google.com/uc?export=download&id=1FTYfLq3abdUP6qXUpqQYRY8Dn1kPBgPF)

- Edit data column "country"

```bash
# Cleansing column "country" by replacing "Japane" to "Japan"
from pyspark.sql.functions import when
dt_clean_country = dt_clean.withColumn("country_update", when(dt_clean["country"] == "Japane", "Japan").otherwise(dt_clean["country"]))
dt_clean = dt_clean_country.drop("country").withColumnRenamed("country_update", "Country")

# Cleansing column "country" by replacing "Philippine" to "Philippines"
dt_clean_Country = dt_clean.withColumn("Country_update", when(dt_clean["Country"] == "Philippine", "Philippines").otherwise(dt_clean["Country"]))
dt_clean = dt_clean_Country.drop("Country").withColumnRenamed("Country_update", "Country")
```
![image.png](https://drive.google.com/uc?export=download&id=1-ihrPtsXvCN-AmmbGpLjxU0meS47BNYQ)

(2.5.2) Semantic Anomalies

- Check column "user_id"

```bash
# Check & Edit column "user_id"
dt_clean_correct_user_id_count = dt_clean.where(dt_clean["user_id"].rlike("^[a-z0-9]{8}$")).count()
print(f"Correct_user_id_counts: {dt_clean_correct_user_id_count}")

dt_correct_userid = dt_clean.where(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid)

# Review dataframe "dt_incorrect_userid"
dt_clean_incorrect_user_id_count = dt_incorrect_userid.count()
print(f"Incorrect_user_id_counts: {dt_clean_incorrect_user_id_count}\n")
dt_incorrect_userid.select("user_id").show()
```

![image.png](https://drive.google.com/uc?export=download&id=10U9ofS2wKIPgMXzKmHS-JOj15A2YXDLl)

- Edit column "user_id"

```bash
# Edit column "user_id" by replacing from "bj03at4600" to "bj03at46"
dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean["user_id"] == "bj03at4600", "bj03at46").otherwise(dt_clean["user_id"]))
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed("user_id_update", "User_ID")

# Replacing from "fx80th7100" to "fx80th71"
dt_clean_UserID = dt_clean.withColumn("User_id_update", when(dt_clean["User_ID"] == "fx80th7100", "fx80th71").otherwise(dt_clean["User_ID"]))
dt_clean = dt_clean_UserID.drop("User_ID").withColumnRenamed("User_id_update", "User_ID")
```

- Recheck column "User_ID" after editing

```bash
# Recheck column "User_ID" after editing
dt_clean_correct_User_ID = dt_clean.where(dt_clean["User_ID"].rlike("^[a-z0-9]{8}$"))
dt_clean_incorrect_User_ID = dt_clean.subtract(dt_clean_correct_User_ID)  

# Review dataframe "dt_clean_incorrect_User_ID"
dt_clean_User_ID_count = dt_clean.where(dt_clean["User_ID"].rlike("^[a-z0-9]{8}$")).count()
print(f"User_ID_counts: {dt_clean_User_ID_count}\n")

# Show incorrect dataframe for rechecking column "User_ID" after editing
dt_clean_incorrect_User_ID.select("User_ID").show()
```

![image.png](https://drive.google.com/uc?export=download&id=13ah86YpfHQiOq9chr5JJ30uwQ9K4IhVi)

(2.5.3) Coverage Anomalies (Missing Value)

- Check missing Value

```bash
# Check missing Value
dt_clean.summary("count").show()

# Check dataframe "dt_clean" is Null value
from pyspark.sql.functions import col, sum
dt_null_list = dt_clean.select([sum(col(colname).isNull().cast("int")).alias(colname) for colname in dt_clean.columns])
dt_null_list.show()

dt_clean.show(10, False)
```
![image.png](https://drive.google.com/uc?export=download&id=1lJNVgEzXQxrxkgSPZgYeVDtdiCoBKF9L)

- Replacing "TBA" value in Columns: "Book Subtitle", "Book Narrator", "Audiobook_Type", "Categories", "Rating" and "Total_No_of_Ratings"

```bash
# Remarks : TBA (To Be Announced)
# Replacing "TBA" value in Columns: "Book Subtitle", "Book Narrator", "Audiobook_Type", "Categories", "Rating" and "Total_No_of_Ratings"

dt_clean_Book_Subtitle = dt_clean.withColumn("Book_Subtitle_update", when(dt_clean["Book Subtitle"].isNull(), "TBA").otherwise(dt_clean["Book Subtitle"]))
dt_clean = dt_clean_Book_Subtitle.drop("Book Subtitle").withColumnRenamed("Book_Subtitle_update", "Book_Subtitle")

dt_clean_Book_Narrator = dt_clean.withColumn("Book_Narrator_update", when(dt_clean["Book Narrator"].isNull(), "TBA").otherwise(dt_clean["Book Narrator"]))
dt_clean = dt_clean_Book_Narrator.drop("Book Narrator").withColumnRenamed("Book_Narrator_update", "Book_Narrator")

dt_clean_Audiobook_Type = dt_clean.withColumn("Audiobook_Type_update", when(dt_clean["Audiobook_Type"].isNull(), "TBA").otherwise(dt_clean["Audiobook_Type"]))
dt_clean = dt_clean_Audiobook_Type.drop("Audiobook_Type").withColumnRenamed("Audiobook_Type_update", "Audiobook_Type")

dt_clean_Categories = dt_clean.withColumn("Categories_update", when(dt_clean["Categories"].isNull(), "TBA").otherwise(dt_clean["Categories"]))
dt_clean = dt_clean_Categories.drop("Categories").withColumnRenamed("Categories_update", "Categories")

# Replacing "0.0" value in Columns: "Rating" and "Total_No_of_Ratings"
dt_clean_Rating = dt_clean.withColumn("Rating_update", when(dt_clean["Rating"].isNull(), "0.0").otherwise(dt_clean["Rating"]))
dt_clean = dt_clean_Rating.drop("Rating").withColumnRenamed("Rating_update", "Rating")

dt_clean_Total_No_of_Ratings = dt_clean.withColumn("Total_No_of_Ratings_update", when(dt_clean["Total_No_of_Ratings"].isNull(), "0.0").otherwise(dt_clean["Total_No_of_Ratings"]))
dt_clean = dt_clean_Total_No_of_Ratings.drop("Total_No_of_Ratings").withColumnRenamed("Total_No_of_Ratings_update", "Total_No_of_Ratings")

# Replacing "00000000" value in Column: "User_ID"
dt_clean_User_ID = dt_clean.withColumn("User_ID_update", when(dt_clean["User_ID"].isNull(), "00000000").otherwise(dt_clean["User_ID"]))
dt_clean = dt_clean_User_ID.drop("User_ID").withColumnRenamed("User_ID_update", "User_id")
```

- Recheck dataframe "dt_clean" after cleansing "Null" value

```bash
# Review dataframe "dt_clean" after cleansing "Null" value
dt_clean.summary("count").show()

from pyspark.sql.functions import col, sum
dt_null_list = dt_clean.select([sum(col(colname).isNull().cast("int")).alias(colname) for colname in dt_clean.columns])
dt_null_list.show()

dt_clean.show(10, False)
```

![image.png](https://drive.google.com/uc?export=download&id=1DiUr63LhSJ-m5aNv0FNRUorVwUnEuD_N)


### 2.6) Save data to CSV

```bash
# Save to csv as a single file
dt_clean.coalesce(1).write.csv("Cleaned_data.csv", header=True)
```

## **Section 3: Upload data from Google Drive to Cloud Storage (GCS) with gcloud SDK**

Login with Google Accounts

```bash
gcloud auth login
```

Upload data from Google Drive to Cloud Storage with gsutil command line

```bash
gsutil cp "G:\My Drive\My data\Cleaned_data.csv" gs://aswbucket
```

![image.png](https://drive.google.com/uc?export=download&id=1BxuhJvfFTzz8_DNNatJw8xZCC4A_6_uL)

Data on GCS

![image.png](https://drive.google.com/uc?export=download&id=1d9MDMW83YCcsqd7bVgyDr5LcDjJmWWzP)


## **Reference on google colab**

[Click Here](https://colab.research.google.com/github/aisawanj/Data_Collection_and_Data_Cleansing/blob/main/Project_01_Data_Collection_with_Pandas_and_Data_Cleansing_with_Spark.ipynb)




## **License**

[Apache License 2.0](https://choosealicense.com/licenses/apache-2.0/)

