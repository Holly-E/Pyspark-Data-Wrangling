# -*- coding: utf-8 -*-
"""
Erickson, Holly

Fitting a Binary Logistic Regression Model to a Dataset

1. Build a Classification Model

Predict the sex of a person based on their age, name, and state

a. Prepare in Input Features
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.feature import OneHotEncoderEstimator
#%%
spark = SparkSession.builder.appName("Week4").getOrCreate()

csv_file_path = 'C://Master/Semester_5/baby-names.csv'
         
df = spark.read.load(
  csv_file_path,
  format="csv",
  sep=",",
  inferSchema=True,
  header=True
)

df.printSchema()

#%%

# StringIndexer along with the OneHotEncoderEstimator to convert the name, state, and sex columns 
#  Use the VectorAssembler to combine the name, state, and age vectors into a single features vector.

categoricalColumns = ['name', 'state', 'sex']
stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
    stages += [stringIndexer, encoder]
label_stringIdx = StringIndexer(inputCol = 'deposit', outputCol = 'label')
stages += [label_stringIdx]
numericCols = ['age', 'balance', 'duration', 'campaign', 'pdays', 'previous']
assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]


indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(df) for column in list(set(df.columns)-set(['year', 'count'])) ]

encoder = OneHotEncoderEstimator(
    inputCols=[indexer.getOutputCol() for indexer in indexers],
    outputCols=["{0}_encoded".format(indexer.getOutputCol()) for indexer in indexers]
)

assembler = VectorAssembler(
    inputCols=['name_index_encoded', 'state_index_encoded', 'year'],
    outputCol="features"
)

pipeline = Pipeline(stages=indexers + [encoder, assembler])
df_r = pipeline.fit(df).transform(df)

df_r.printSchema()

#%%

#Your final dataset should contain a column called features containing the prepared vector 
#and a column called label containing the sex of the person.

keep = ['features', 'sex_index']
df_final = df_r.select([column for column in df_r.columns if column in keep])
df_final = df_final.withColumnRenamed("sex_index","label")
df_final.printSchema()

#%%
#2. Fit and Evaluate the Model
#Fit the model as a logistic regression model with the following parameters.
# LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8).
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lrModel = lr.fit(df_final)

# Provide the area under the ROC curve for the model.
trainingSummary = lrModel.summary
print('Area Under ROC: ' + str(trainingSummary.areaUnderROC))


"""
Output
Area Under ROC: 0.5
"""
