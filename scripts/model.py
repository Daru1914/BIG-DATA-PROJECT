"""
This module processes our datasets and builds two ML models: LR and DTR
"""
import pyspark.sql.functions as F
import pyspark.ml.feature as feats
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder\
    .appName("BDT Project")\
    .config("spark.sql.catalogImplementation","hive")\
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .config('spark.driver.memory','4G')\
    .enableHiveSupport()\
    .getOrCreate()


sc = spark.sparkContext

menus = spark.read.format("avro").table('projectdb.menus')
menus.createOrReplaceTempView('menus')

restaurants = spark.read.format("avro").table('projectdb.restaurants')
restaurants.createOrReplaceTempView('restaurants')

menus_part = spark.read.format("avro").table('projectdb.menus_part')
menus_part.createOrReplaceTempView('menus_part')

menus.printSchema()
restaurants.printSchema()
menus_part.printSchema()

# only choose columns that can directly impact the price and the label itself,
# ids are kept for union later

features_menus = ['restaurant_id', 'category', 'name', 'price']
features_restaurants = ['id', 'score', 'category', 'ratings', 'price_range', 'lat', 'lng']

LABEL = 'price'

menus_post_process = menus.select(features_menus)

# dropping rows with no score or ratings data
restaurants_post_process = restaurants.select(features_restaurants)
print('Before dropping nans: ' + str(restaurants_post_process.count()))
restaurants_post_process = restaurants_post_process.filter((F.col('score') != "")\
                                                            & (F.col('ratings') != "")\
                                                            & (F.col('price_range') != ""))
print('After dropping nans: ' + str(restaurants_post_process.count()))

# transform score, ratings, lat and lng to numeric
floaty_features = ['score', 'ratings', 'lat', 'lng']
for column in floaty_features:
    restaurants_post_process = restaurants_post_process\
        .withColumn(column, F.col(column).cast('float'))

# convert category column to list, explode it, one-hot encode later
restaurants_post_process = restaurants_post_process\
    .withColumn('category', F.split(restaurants_post_process['category'], ','))
restaurants_post_process = restaurants_post_process\
    .withColumn('category', F.explode('category'))

categoricalCols = ['category', 'price_range']
categoricalInds = ['categoryIndex', 'price_rangeIndex']
categoricalVecs = ['category_vec', 'price_range_vec']

transform_empty = F.udf(lambda s: "NA" if s == "" else s, StringType())
for col in categoricalCols:
    restaurants_post_process = restaurants_post_process\
        .withColumn(col, transform_empty(col))

indexer = feats.StringIndexer(inputCols=categoricalCols, outputCols=categoricalInds)
restaurants_post_process = indexer.fit(restaurants_post_process)\
    .transform(restaurants_post_process)

encoder = feats.OneHotEncoder(inputCols=["categoryIndex", "price_rangeIndex"],
                              outputCols=["category_vec", "price_range_vec"])
restaurants_post_process = encoder.fit(restaurants_post_process)\
    .transform(restaurants_post_process)
restaurants_post_process = restaurants_post_process\
    .drop('category', 'price_range', 'categoryIndex', 'price_rangeIndex')

# let us now join the two tables

final_df = restaurants_post_process\
    .join(menus_post_process,
          restaurants_post_process.id == menus_post_process.restaurant_id, "inner")

# drop the indices, not needed after join
final_df = final_df.drop("id", "restaurant_id")

# final preprocessing steps, encoding names and categories
tokenizer_1 = feats.RegexTokenizer(inputCol="category",
                                   outputCol="category_tokens",
                                   pattern=" ")
tokenizer_2 = feats.RegexTokenizer(inputCol="name",
                                   outputCol="name_tokens",
                                   pattern=" ")
word2Vec_1 = feats.Word2Vec(vectorSize=5, seed=42, minCount=1,
                            inputCol="category_tokens", outputCol="category_enc")
word2Vec_2 = feats.Word2Vec(vectorSize=10, seed=32, minCount=1,
                            inputCol="name_tokens", outputCol="name_enc")

test_df = tokenizer_1.transform(final_df)
test_df = tokenizer_2.transform(test_df)
test_df = word2Vec_1.fit(test_df).transform(test_df)
test_df = word2Vec_2.fit(test_df).transform(test_df)

test_df = test_df.drop("category", "name", "category_tokens", "name_tokens")

REGEX_PATTERN = r'^(\d+\.\d+)'
test_df = test_df\
    .withColumn('label', F.regexp_extract(F.col('price'), REGEX_PATTERN, 1).cast('float'))
final_df = test_df.drop('price')

# 70-30 split
(trainingData, testData) = final_df.randomSplit([0.7, 0.3])

# model building

# model 1 - Linear Regression
# Create a VectorAssembler to combine features into a single vector column
assembler = VectorAssembler(inputCols=["score", "ratings", "lat",
                                       "lng", "category_vec", "price_range_vec", 
                                       "category_enc", "name_enc"], outputCol="features", 
                                       handleInvalid="skip")

# Create a Linear Regression estimator
lr = LinearRegression()

# Define the hyperparameter grid for the model
param_grid = ParamGridBuilder().addGrid(lr.regParam, [0.01, 0.1, 1.0])\
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]).build()

# Define the cross-validation object
crossval = CrossValidator(estimator=lr, estimatorParamMaps=param_grid,
                          evaluator=RegressionEvaluator(metricName="rmse"), numFolds=4)

trainingData = trainingData.na.drop()

# Train the model on the training data
assembled_train_data = assembler.transform(trainingData)
assembled_train_data = assembled_train_data.drop("score", "ratings", "lat",
                                                 "lng", "category_vec", 
                                                 "price_range_vec", 
                                                 "category_enc", "name_enc")
assembled_train_data = assembled_train_data.na.drop()
MODEL = crossval.fit(assembled_train_data)

testData = testData.na.drop()
# Test the model on the test data
assembled_test_data = assembler.transform(testData)
assembled_test_data = assembled_test_data.drop("score", "ratings", "lat",
                                               "lng", "category_vec", 
                                               "price_range_vec", 
                                               "category_enc", "name_enc")
assembled_test_data = assembled_test_data.drop("score", "ratings", "lat",
                                               "lng", "category_vec", 
                                               "price_range_vec", 
                                               "category_enc", "name_enc")
predictions = MODEL.transform(assembled_test_data)

# Evaluate the model performance on the test data
evaluator = RegressionEvaluator(metricName="rmse")
rmse = evaluator.evaluate(predictions)

print("LR's cross-val's Root Mean Squared Error (RMSE) on test data = %g" % rmse)
# Get the best model hyperparameters
best_lr_model_params = MODEL.bestModel.extractParamMap()
reg_param = best_lr_model_params[MODEL.RegParam]
elastic_net_param = best_lr_model_params[MODEL.ElasticNetParam]
print("Best LR hyperparameters: regParam = %g, elasticNetParam = %g"\
      % (reg_param, elastic_net_param))

# train and test linear regression
lr = LinearRegression(featuresCol='features', labelCol='label',
                      regParam=0.01, elasticNetParam=0)
lrModel = lr.fit(assembled_train_data)
predictions = lrModel.transform(assembled_test_data)
evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='label')
rmse = evaluator.evaluate(predictions, {evaluator.metricName: 'rmse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print("LR RMSE: {}".format(rmse))
print("LR R^2: {}".format(r2))

# Create a DecisionTreeRegressor object
dt = DecisionTreeRegressor()
# Define the hyperparameter grid
paramGrid = ParamGridBuilder().addGrid(dt.maxDepth, [2, 5, 10]).\
    addGrid(dt.minInstancesPerNode, [1, 5, 10]).build()
# Create a cross-validator object
crossval = CrossValidator(estimator=dt, estimatorParamMaps=paramGrid,
                          evaluator=RegressionEvaluator(metricName="rmse"),
                          numFolds=4, seed=42)

# Fit the model and find the best hyperparameters
CV_MODEL = crossval.fit(assembled_train_data)
cvPredictions = CV_MODEL.transform(assembled_test_data)
cvPredictions = CV_MODEL.transform(assembled_test_data)

cvRmse = evaluator.evaluate(cvPredictions)

print("DTR's cross-val's Root Mean Squared Error (RMSE) on test data = %g" % cvRmse)

# Get the best model hyperparameters
best_dtr_model_params = CV_MODEL.bestModel.extractParamMap()
max_depth = best_lr_model_params[CV_MODEL.MaxDepth]
min_instances_per_node = best_lr_model_params[CV_MODEL.MinInstancesPerNode]
print("Best DTR hyperparameters: maxDepth = %g, minInstancesPerNode = %g" \
      % (max_depth, min_instances_per_node))

# train and test decision tree regressor
dtr = DecisionTreeRegressor(featuresCol='features', labelCol='label',
                            maxDepth = 10, minInstancesPerNode = 1)
dtrModel = dtr.fit(assembled_train_data)
dtrPredictions = dtrModel.transform(assembled_test_data)
dtrRmse = evaluator.evaluate(dtrPredictions, {evaluator.metricName: 'rmse'})
dtrR2 = evaluator.evaluate(dtrPredictions, {evaluator.metricName: 'r2'})
print("DTR RMSE: {}".format(dtrRmse))
print("DTR R2: {}".format(dtrR2))

# save the outputs
predictions.coalesce(1).select("prediction",'label').write.mode("overwrite")\
    .format("csv").option("sep", ",").option("header","true")\
        .csv("/project/output/lr_predictions.csv")
dtrPredictions.coalesce(1).select("prediction",'label').write.mode("overwrite")\
    .format("csv").option("sep", ",").option("header","true")\
        .csv("/project/output/dtr_predictions.csv")
