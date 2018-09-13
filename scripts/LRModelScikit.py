class LRModelScikit:
    
    # execute LRModel
    
    def execute(self,spark,filename):
        
        import pandas as pd
        import numpy as np
        
        # Reading the data from hdfs
        data = spark.read.option("delimiter",",").option("header","false").csv(filename)
        dataset = data.toPandas()
        dataset = dataset.iloc[:,:2]
        
        message = dataset['_c1']
        
        # Creating the Bag of Words model
        from sklearn.feature_extraction.text import CountVectorizer
        cv = CountVectorizer(max_features = 1500)
        X = cv.fit_transform(message).toarray()
        y = dataset.iloc[:, 0].values
        
        # Splitting the dataset into the Training set and Test set
        from sklearn.cross_validation import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.20, random_state = 0)
        
        #Fitting Logistic Regression to the training set
        from sklearn.linear_model import LogisticRegression
        classifier = LogisticRegression(random_state=0)
        classifier.fit(X_train,y_train)
        
        #Predicting the Test Results
        y_pred = classifier.predict(X_test)
        
        #Printing the model accuracy
        from sklearn.metrics import accuracy_score
        print('Accuracy: %.2f%%' % (accuracy_score(y_test, y_pred) * 100))
        
        #Making the Confusion Matrix
        from sklearn.metrics import confusion_matrix
        cm = confusion_matrix(y_test,y_pred)
        
        #Printing the confusion matrix in a table
        x=pd.Series(np.array([1,0,1,0]))
        y=pd.Series(np.array([1,1,0,0]))
        z=pd.Series(np.array([cm[0][0],cm[0][1],cm[1][0],cm[1][1]]))
        
        cm_df = pd.DataFrame({'y_test':x,'y_pred':y,'count':z})
        cm_df = cm_df[['y_test','y_pred','count']]
        print cm_df.to_string(index=False)