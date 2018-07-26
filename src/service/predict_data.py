import datetime
import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession


def random_forest_classifier(training_data, testing_data):
    assembler = VectorAssembler(
        inputCols=["col1", "col2", "col3", "col4", "col5", "col6", "col7",
                   "col8", "col9", "col10", "col11", "col12", "col13", "col14",
                   "col15", "col16", "col17", "col19", "col20", "col21",
                   "col22", "col23", "col24", "col25"],
        outputCol="features")
    training_data_vector = assembler.transform(training_data)
    training_data_vector = training_data_vector.select('index', 'features', 'label')

    label_indexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(training_data_vector)
    feature_indexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=15).fit(
        training_data_vector)

    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=10)
    pipeline = Pipeline(stages=[label_indexer, feature_indexer, rf])

    param_grid = ParamGridBuilder() \
        .addGrid(feature_indexer.maxCategories, [5, 15, 25]) \
        .addGrid(rf.numTrees, [10, 50, 100]) \
        .addGrid(rf.maxDepth, [5, 10])

    tvs = TrainValidationSplit(estimator=pipeline,
                               estimatorParamMaps=param_grid,
                               evaluator=RegressionEvaluator(),
                               trainRatio=0.8)

    model = tvs.fit(training_data_vector)

    testing_data_vector = assembler.transform(testing_data)

    predictions = model.transform(testing_data_vector.select('index', 'features', 'bid', 'target'))

    selected = predictions.select("index", 'bid', 'target', "probability", "prediction")

    __output('RandomForestClassifier', selected)


def gbt_classifier(training_data, testing_data):
    assembler = VectorAssembler(
        inputCols=["col1", "col2", "col3", "col4", "col5", "col6", "col7",
                   "col8", "col9", "col10", "col11", "col12", "col13", "col14",
                   "col15", "col16", "col17", "col19", "col20", "col21",
                   "col22", "col23", "col24", "col25"],
        outputCol="features")
    training_data_vector = assembler.transform(training_data)
    training_data_vector = training_data_vector.select('index', 'features', 'label')

    label_indexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(training_data_vector)
    feature_indexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(
        training_data_vector)

    rf = GBTClassifier(labelCol="indexedLabel", featuresCol="features", maxIter=10)
    pipeline = Pipeline(stages=[label_indexer, feature_indexer, rf])

    param_grid = ParamGridBuilder() \
        .addGrid(feature_indexer.maxCategories, [5, 10, 20]) \
        .addGrid(rf.maxDepth, [5, 10]) \
        .addGrid(rf.maxIter, [10, 20]) \
        .build()

    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=param_grid,
                              evaluator=BinaryClassificationEvaluator(),
                              numFolds=3)

    model = crossval.fit(training_data_vector)

    testing_data_vector = assembler.transform(testing_data)

    predictions = model.transform(testing_data_vector.select('index', 'features', 'bid', 'target'))

    selected = predictions.select("index", 'bid', 'target', "probability", "prediction")

    __output('GBT_classifier', selected)


def __load_data():
    spark = SparkSession.builder.appName('random_forest_classifier').getOrCreate()
    training_json_data = spark.read.format('json').load('../../data/clean_data')
    training_json_data.createOrReplaceTempView('stock')
    training_data = spark.sql("""select index, label, 
                       first.col1 as col1, first.col2 as col2, first.col3 as col3, first.col4 as col4, 
                       first.col5 as col5, first.col6 as col6, first.col7 as col7, first.col8 as col8, 
                       second.col1 as col9, second.col2 as col10, second.col3 as col11, second.col4 as col12, 
                       second.col5 as col13, second.col6 as col14, second.col7 as col15, second.col8 as col16,
                       third.col1 as col17, third.col2 as col19, third.col3 as col20, third.col4 as col21, 
                       third.col5 as col22, third.col6 as col23, third.col7 as col24, third.col8 as col25 
                       from stock""")
    test_json_data = spark.read.format('json').load('../../data/test_data')
    test_json_data.createOrReplaceTempView('test_stock')
    testing_data = spark.sql("""select index,  
                                    first.col1 as col1, first.col2 as col2, first.col3 as col3, first.col4 as col4, 
                                    first.col5 as col5, first.col6 as col6, first.col7 as col7, first.col8 as col8, 
                                    second.col1 as col9, second.col2 as col10, second.col3 as col11, second.col4 as col12, 
                                    second.col5 as col13, second.col6 as col14, second.col7 as col15, second.col8 as col16,
                                    third.col1 as col17, third.col2 as col19, third.col3 as col20, third.col4 as col21, 
                                    third.col5 as col22, third.col6 as col23, third.col7 as col24, third.col8 as col25, 
                                    bid, target 
                                    from test_stock""")
    return training_data, testing_data


def __output(model_type, selected):
    result_data_path = '../../data/result_data/' + model_type + '_' + datetime.date.today().strftime(
        '%Y-%m-%d') + '.json'
    if os.path.exists(result_data_path):
        os.remove(result_data_path)
    rs_list = []
    for row in selected.collect():
        rid, bid, target, prob, prediction = row
        if prediction > 0.0:
            rs_list.append(
                {'code': rid.split('-')[1], 'bidPrice': str(bid), 'target': str(target), 'prob': str(prob)})
            print("(%s, 参考买入价=%s, 参考卖出价=%s) --> 概率=%s, 预测结果=%f" % (
                rid.split('-')[1], str(bid), str(target), str(prob), prediction))
    with open(result_data_path, 'wb') as f:
        f.write(json.dumps(rs_list).encode('UTF-8'))


def start():
    (training_data, testing_data) = __load_data()
    random_forest_classifier(training_data, testing_data)
    gbt_classifier(training_data, testing_data)


if __name__ == '__main__':
    start()
