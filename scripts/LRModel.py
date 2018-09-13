class LRModel:
    
    # execute LRModel
    
    def execute(self,sc,filename,pipeline):
        
        # Loading the Data
        smsData = sc.textFile(filename)
        smsData.cache()
        
        # creating a labeled vector
        def TransformToVector(string):
            attList = string.split(",")
            smsType = 0.0 if attList[0] == "ham" else 1.0
            return [smsType,attList[1]]
        
        smsTransformed = smsData.map(TransformToVector)
        
        # creating a data frame from labeled vector
        from pyspark.sql import SQLContext
        sqlContext = SQLContext(sc)
        smsDF = sqlContext.createDataFrame(smsTransformed,["label","message"])
        smsDF.cache()
        
        # split data frame into training and testing
        (trainingData,testData) = smsDF.randomSplit([0.9,0.1])
        
        #Build a model with Pipeline
        lrModel = pipeline.fit(trainingData)
        
        #Compute Predictions
        prediction = lrModel.transform(testData)
            
        #Evaluate Accuracy
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", \
                                                      labelCol="label", \
                                                      metricName = "accuracy")
        accuracy = evaluator.evaluate(prediction)
        print "Model Accuracy: " + str(round(accuracy*100,2))
        
        # Draw a confusion matrix
        prediction.groupby("label","prediction").count().show()
