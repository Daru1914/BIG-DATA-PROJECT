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

SPARK = SparkSession.builder\
    .appName("BDT Project")\
    .config("spark.sql.catalogImplementation", "hive")\
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
    .config("spark.sql.avro.compression.codec", "snappy")\
    .config('spark.driver.memory', '4G')\
    .enableHiveSupport()\
    .getOrCreate()


SC = SPARK.sparkContext

MENUS = SPARK.read.format("avro").table('projectdb.menus')
MENUS.createOrReplaceTempView('menus')

RESTAURANTS = SPARK.read.format("avro").table('projectdb.restaurants')
RESTAURANTS.createOrReplaceTempView('restaurants')

MENUS_PART = SPARK.read.format("avro").table('projectdb.menus_part')
MENUS_PART.createOrReplaceTempView('menus_part')

MENUS.printSchema()
RESTAURANTS.printSchema()
MENUS_PART.printSchema()

# only choose columns that can directly impact the price and the label itself,
# ids are kept for union later

FEATURES_MENUS = ['restaurant_id', 'category', 'name', 'price']
FEATURES_RESTAURANTS = ['id', 'score', 'category', 'ratings', 'price_range', 'lat', 'lng']

LABEL = 'price'

MENUS_POST_PROCESS = MENUS.select(FEATURES_MENUS)

# dropping rows with no score or ratings data
RESTAURANTS_POST_PROCESS = RESTAURANTS.select(FEATURES_RESTAURANTS)
print 'Before dropping nans: ' + str(RESTAURANTS_POST_PROCESS.count())
RESTAURANTS_POST_PROCESS = RESTAURANTS_POST_PROCESS\
    .filter((RESTAURANTS_POST_PROCESS.score != "")\
             & (RESTAURANTS_POST_PROCESS.ratings != "")\
             & (RESTAURANTS_POST_PROCESS.price_range != ""))
print 'After dropping nans: ' + str(RESTAURANTS_POST_PROCESS.count())

# transform score, ratings, lat and lng to numeric
FLOATY_FEATURES = ['score', 'ratings', 'lat', 'lng']
for column in FLOATY_FEATURES:
    RESTAURANTS_POST_PROCESS = RESTAURANTS_POST_PROCESS\
        .withColumn(column, RESTAURANTS_POST_PROCESS[column].cast('float'))

# convert category column to list, explode it, one-hot encode later
RESTAURANTS_POST_PROCESS = RESTAURANTS_POST_PROCESS\
    .withColumn('category', F.split(RESTAURANTS_POST_PROCESS['category'], ','))
RESTAURANTS_POST_PROCESS = RESTAURANTS_POST_PROCESS\
    .withColumn('category', F.explode('category'))

CATEGORICAL_COLS = ['category', 'price_range']
CATEGORICAL_INDS = ['categoryIndex', 'price_rangeIndex']
CATEGORICAL_VECS = ['category_vec', 'price_range_vec']

TRANSFORM_EMPTY = F.udf(lambda s: "NA" if s == "" else s, StringType())
for col in CATEGORICAL_COLS:
    RESTAURANTS_POST_PROCESS = RESTAURANTS_POST_PROCESS\
        .withColumn(col, TRANSFORM_EMPTY(col))

INDEXER = feats.StringIndexer(inputCols=CATEGORICAL_COLS, outputCols=CATEGORICAL_INDS)
RESTAURANTS_POST_PROCESS = INDEXER.fit(RESTAURANTS_POST_PROCESS)\
    .transform(RESTAURANTS_POST_PROCESS)

ENCODER = feats.OneHotEncoder(inputCols=["categoryIndex", "price_rangeIndex"],
                              outputCols=["category_vec", "price_range_vec"])
RESTAURANTS_POST_PROCESS = ENCODER.fit(RESTAURANTS_POST_PROCESS)\
    .transform(RESTAURANTS_POST_PROCESS)
RESTAURANTS_POST_PROCESS = RESTAURANTS_POST_PROCESS\
    .drop('category', 'price_range', 'categoryIndex', 'price_rangeIndex')

# let us now join the two tables

FINAL_DF = RESTAURANTS_POST_PROCESS\
    .join(MENUS_POST_PROCESS,
          RESTAURANTS_POST_PROCESS.id == MENUS_POST_PROCESS.restaurant_id, "inner")

# drop the indices, not needed after join
FINAL_DF = FINAL_DF.drop("id", "restaurant_id")

# final preprocessing steps, encoding names and categories
TOKENIZER_1 = feats.RegexTokenizer(inputCol="category",
                                   outputCol="category_tokens",
                                   pattern=" ")
TOKENIZER_2 = feats.RegexTokenizer(inputCol="name",
                                   outputCol="name_tokens",
                                   pattern=" ")
WORD2VEC_1 = feats.Word2Vec(vectorSize=5, seed=42, minCount=1,
                            inputCol="category_tokens", outputCol="category_enc")
WORD2VEC_2 = feats.Word2Vec(vectorSize=10, seed=32, minCount=1,
                            inputCol="name_tokens", outputCol="name_enc")

TEST_DF = TOKENIZER_1.transform(FINAL_DF)
TEST_DF = TOKENIZER_2.transform(TEST_DF)
TEST_DF = WORD2VEC_1.fit(TEST_DF).transform(TEST_DF)
TEST_DF = WORD2VEC_2.fit(TEST_DF).transform(TEST_DF)

TEST_DF = TEST_DF.drop("category", "name", "category_tokens", "name_tokens")

REGEX_PATTERN = r'^(\d+\.\d+)'
TEST_DF = TEST_DF\
    .withColumn('label', F.regexp_extract(TEST_DF.price, REGEX_PATTERN, 1).cast('float'))
FINAL_DF = TEST_DF.drop('price')

# 70-30 split
(TRAINING_DATA, TEST_DATA) = FINAL_DF.randomSplit([0.7, 0.3])

# model building

# model 1 - Linear Regression
# Create a VectorAssembler to combine features into a single vector column
ASSEMBLER = VectorAssembler(inputCols=["score", "ratings", "lat",
                                       "lng", "category_vec", "price_range_vec",
                                       "category_enc", "name_enc"], outputCol="features",
                            handleInvalid="skip")

# Create a Linear Regression estimator
LR = LinearRegression()

# Define the hyperparameter grid for the model
PARAM_GRID = ParamGridBuilder().addGrid(LR.regParam, [0.01, 0.1, 1.0])\
    .addGrid(LR.elasticNetParam, [0.0, 0.5, 1.0]).build()

# Define the cross-validation object
CROSSVAL = CrossValidator(estimator=LR, estimatorParamMaps=PARAM_GRID,
                          evaluator=RegressionEvaluator(metricName="rmse"), numFolds=4)

TRAINING_DATA = TRAINING_DATA.na.drop()

# Train the model on the training data
ASSEMBLED_TRAIN_DATA = ASSEMBLER.transform(TRAINING_DATA)
ASSEMBLED_TRAIN_DATA = ASSEMBLED_TRAIN_DATA.drop("score", "ratings", "lat",
                                                 "lng", "category_vec",
                                                 "price_range_vec",
                                                 "category_enc", "name_enc")
ASSEMBLED_TRAIN_DATA = ASSEMBLED_TRAIN_DATA.na.drop()
MODEL = CROSSVAL.fit(ASSEMBLED_TRAIN_DATA)

TEST_DATA = TEST_DATA.na.drop()
# Test the model on the test data
ASSEMBLED_TEST_DATA = ASSEMBLER.transform(TEST_DATA)
ASSEMBLED_TEST_DATA = ASSEMBLED_TEST_DATA.drop("score", "ratings", "lat",
                                               "lng", "category_vec",
                                               "price_range_vec",
                                               "category_enc", "name_enc")
ASSEMBLED_TEST_DATA = ASSEMBLED_TEST_DATA.drop("score", "ratings", "lat",
                                               "lng", "category_vec",
                                               "price_range_vec",
                                               "category_enc", "name_enc")
PREDICTIONS = MODEL.transform(ASSEMBLED_TEST_DATA)

# Evaluate the model performance on the test data
EVALUATOR = RegressionEvaluator(metricName="rmse")
RMSE = EVALUATOR.evaluate(PREDICTIONS)

print "LR's cross-val's Root Mean Squared Error (RMSE) on test data = %g" % RMSE
# Get the best model hyperparameters
REG_PARAM = MODEL.bestModel.extractParamMap()[MODEL.bestModel.getParam("regParam")]
ELASTIC_NET_PARAM = MODEL.bestModel.extractParamMap()[MODEL.bestModel.getParam("elasticNetParam")]
print "Best LR hyperparameters: regParam = %g, elasticNetParam = %g"\
      % (REG_PARAM, ELASTIC_NET_PARAM)

# train and test linear regression
LR = LinearRegression(featuresCol='features', labelCol='label',
                      regParam=0.01, elasticNetParam=0)
LR_MODEL = LR.fit(ASSEMBLED_TRAIN_DATA)
PREDICTIONS = LR_MODEL.transform(ASSEMBLED_TEST_DATA)
EVALUATOR = RegressionEvaluator(predictionCol='prediction', labelCol='label')
RMSE = EVALUATOR.evaluate(PREDICTIONS, {EVALUATOR.metricName: 'rmse'})
R2 = EVALUATOR.evaluate(PREDICTIONS, {EVALUATOR.metricName: 'r2'})
print "LR RMSE: {}".format(RMSE)
print "LR R^2: {}".format(R2)
LR_MODEL.save("model")

# Create a DecisionTreeRegressor object
DT = DecisionTreeRegressor()
# Define the hyperparameter grid
PARAM_GRID = ParamGridBuilder().addGrid(DT.maxDepth, [2, 5, 10]).\
    addGrid(DT.minInstancesPerNode, [1, 5, 10]).build()
# Create a cross-validator object
CROSSVAL = CrossValidator(estimator=DT, estimatorParamMaps=PARAM_GRID,
                          evaluator=RegressionEvaluator(metricName="rmse"),
                          numFolds=4, seed=42)

# Fit the model and find the best hyperparameters
CV_MODEL = CROSSVAL.fit(ASSEMBLED_TRAIN_DATA)
CV_PREDICTIONS = CV_MODEL.transform(ASSEMBLED_TEST_DATA)
CV_PREDICTIONS = CV_MODEL.transform(ASSEMBLED_TEST_DATA)

CV_RMSE = EVALUATOR.evaluate(CV_PREDICTIONS)

print "DTR's cross-val's Root Mean Squared Error (RMSE) on test data = %g" % CV_RMSE

# Get the best model hyperparameters
MAX_DEPTH = CV_MODEL.bestModel.extractParamMap()[CV_MODEL.bestModel.getParam("maxDepth")]
MAX_INSTANCES_PER_NODE = CV_MODEL.bestModel.extractParamMap()[CV_MODEL.bestModel.\
                                                              getParam("minInstancesPerNode")]
print "Best DTR hyperparameters: maxDepth = %g, minInstancesPerNode = %g" \
      % (MAX_DEPTH, MAX_INSTANCES_PER_NODE)

# train and test decision tree regressor
DTR = DecisionTreeRegressor(featuresCol='features', labelCol='label',
                            maxDepth=10, minInstancesPerNode=1)
DTR_MODEL = DTR.fit(ASSEMBLED_TRAIN_DATA)
DTR_PREDICTIONS = DTR_MODEL.transform(ASSEMBLED_TEST_DATA)
DTR_RMSE = EVALUATOR.evaluate(DTR_PREDICTIONS, {EVALUATOR.metricName: 'rmse'})
DTR_2 = EVALUATOR.evaluate(DTR_PREDICTIONS, {EVALUATOR.metricName: 'r2'})
print "DTR RMSE: {}".format(DTR_RMSE)
print "DTR R2: {}".format(DTR_2)
DTR_MODEL.save("model")

# save the outputs
PREDICTIONS.coalesce(1).select("prediction", 'label').write.mode("overwrite")\
    .format("csv").option("sep", ",").option("header", "true")\
        .csv("/project/output/lr_predictions.csv")
DTR_PREDICTIONS.coalesce(1).select("prediction", 'label').write.mode("overwrite")\
    .format("csv").option("sep", ",").option("header", "true")\
        .csv("/project/output/dtr_predictions.csv")
