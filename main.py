# %%
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import mean,col,split, col, regexp_extract, when, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import QuantileDiscretizer

# %%
spark = SparkSession.builder.appName("Bank").getOrCreate()

# %%
df = spark.read.csv('bank.csv', header=True, inferSchema=True)

# %%
df.cache()

# %%
df.is_cached

# %%
df.printSchema()

# %%
df.count()

# %%
len(df.columns)

# %%
catCols = ['job', 'marital', 'education', 'default','housing', 'loan', 'contact', 'poutcome']
indexers = [
    StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c))
    for c in catCols
]
encoders = [OneHotEncoder(dropLast=False,inputCol=indexer.getOutputCol(),
            outputCol="{0}_encoded".format(indexer.getOutputCol())) 
    for indexer in indexers
]
assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders],outputCol="rawFeatures")

numericCols = ['age', 'balance', 'duration',  'campaign', 'pdays', 'previous']

pipeline = Pipeline(stages=indexers + encoders+ [assembler])
model=pipeline.fit(df)
transformed = model.transform(df)
transformed.show(5)

# %%
(trainingData, testData) = transformed.randomSplit([0.7, 0.3],seed = 11)

# %%
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(labelCol="loan_indexed", featuresCol="rawFeatures")
#Training algo
lrModel = lr.fit(trainingData)
lr_prediction = lrModel.transform(testData)
lr_prediction.select("prediction", "loan_indexed", "rawFeatures").show()
evaluator = MulticlassClassificationEvaluator(labelCol="loan_indexed", predictionCol="prediction", metricName="accuracy")

# %%
lr_accuracy = evaluator.evaluate(lr_prediction)
print("Accuracy of LogisticRegression is = %g"% (lr_accuracy))
print("Test Error of LogisticRegression = %g " % (1.0 - lr_accuracy))


