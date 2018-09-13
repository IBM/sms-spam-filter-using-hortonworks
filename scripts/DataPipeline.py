class DataPipeline:
    
    #Setup pipeline
    
    def pipeline(self):
          
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import HashingTF, IDF    
        from pyspark.ml.feature import Tokenizer
        from pyspark.ml.classification import LogisticRegression
    
        tokenizer = Tokenizer(inputCol="message",outputCol="words")
        hashingTF = HashingTF(inputCol = tokenizer.getOutputCol(),outputCol="tempfeatures")
        idf = IDF(inputCol = hashingTF.getOutputCol(),outputCol="features")
        lrClassifier = LogisticRegression()
        
        pipeline = Pipeline(stages=[tokenizer,hashingTF,idf,lrClassifier])
        
        return pipeline