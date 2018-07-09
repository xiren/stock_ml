import datetime
import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.sql import SparkSession


def start():
    spark = SparkSession.builder.appName('random_forest_classifier').getOrCreate()

    data = spark.read.format('json').load('../../data/clean_data')
    data.createOrReplaceTempView('stock')
    org_data = spark.sql("""select index, label, 
                   first.col1 as col1, first.col2 as col2, first.col3 as col3, first.col4 as col4, 
                   first.col5 as col5, first.col6 as col6, first.col7 as col7, first.col8 as col8, 
                   second.col1 as col9, second.col2 as col10, second.col3 as col11, second.col4 as col12, 
                   second.col5 as col13, second.col6 as col14, second.col7 as col15, second.col8 as col16,
                   third.col1 as col17, third.col2 as col19, third.col3 as col20, third.col4 as col21, 
                   third.col5 as col22, third.col6 as col23, third.col7 as col24, third.col8 as col25 
                   from stock""")

    assembler = VectorAssembler(
        inputCols=["col1", "col2", "col3", "col4", "col5", "col6", "col7",
                   "col8", "col9", "col10", "col11", "col12", "col13", "col14",
                   "col15", "col16", "col17", "col19", "col20", "col21",
                   "col22", "col23", "col24", "col25"],
        outputCol="features")
    org_data = assembler.transform(org_data)
    training = org_data.select('index', 'features', 'label')

    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(training)
    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(training)
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=150)
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf])

    model = pipeline.fit(training)

    test_data = spark.read.format('json').load('../../data/test_data')
    test_data.createOrReplaceTempView('test_stock')
    testing = spark.sql("""select index,  
                                first.col1 as col1, first.col2 as col2, first.col3 as col3, first.col4 as col4, 
                                first.col5 as col5, first.col6 as col6, first.col7 as col7, first.col8 as col8, 
                                second.col1 as col9, second.col2 as col10, second.col3 as col11, second.col4 as col12, 
                                second.col5 as col13, second.col6 as col14, second.col7 as col15, second.col8 as col16,
                                third.col1 as col17, third.col2 as col19, third.col3 as col20, third.col4 as col21, 
                                third.col5 as col22, third.col6 as col23, third.col7 as col24, third.col8 as col25, 
                                bid, target 
                                from test_stock""")
    testing = assembler.transform(testing)

    predictions = model.transform(testing.select('index', 'features', 'bid', 'target'))

    selected = predictions.select("index", 'bid', 'target', "probability", "prediction")
    result_data_path = '../../data/result_data/' + datetime.date.today().strftime('%Y-%m-%d') + '.json'
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


if __name__ == '__main__':
    start()
