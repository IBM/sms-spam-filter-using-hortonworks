# Discover, Train and Deploy Spam Filter Model on Hortonworks Data Platform using DSX Local

The goal of this code pattern is to demonstrate how data scientists can leverage remote spark clusters and compute environments from Hortonworks Data Platform (HDP) to train and deploy a Spam Filter Model using IBM's Data Science Experience Local (DSX Local).

Spam Filter is a classification model built using natural language processing and machine learning algorithms. The model is trained on the [sms spam collection dataset](https://www.kaggle.com/ishansoni/sms-spam-collection-dataset) to classify whether a given text message is spam or ham (not spam).

There are three different examples or illustrations to the problem:

Note: All the development and training including those in HDP cluster are done within the IBM's DSX Local

* Develop a spam filter model using local spark ML (provided by DSX Local), train and deploy the model leveraging the remote spark in HDP cluster within DSX Local

* Develop a spam filter model using thrid pary library (Scikit-learn) in DSX Local, train and deploy the model leveraging the compute in remote HDP cluster within DSX Local

* Package the spam filter model as a python egg in DSX Local, train and deploy the model package levaraging both the remote spark and compute in HDP cluster within DSX Local

In order for the DSX Local to be able to utilize the resources (spark, compute) of the HDP cluster, DSX Hadoop Integration Service (DSXHI) must be installed on the edge node of the HDP cluster and the DSXHI must be registered with the DSX Local. 

> **What is HDP?** Hortonworks Data Platform (HDP) is a massively scalable platform for storing, processing and analyzing large volumes of data. HDP consists of the essential set of Apache Hadoop projects including MapReduce, Hadoop Distributed File System (HDFS), HCatalog, Pig, Hive, HBase, Zookeeper and Ambari.

  ![](doc/source/images/hdp_arch.png)

   *Hortonworks Data Platform by [Hortonworks](https://hortonworks.com/products/data-platforms/hdp/)*

> **What is IBM DSX Local?** DSX Local is an on premises solution for data scientists and data engineers. It offers a suite of data science tools that integrate with RStudio, Spark, Jupyter, and Zeppelin notebook technologies. And yes, it can be configured to use HDP, too.

> **What is the IBM DSXHI?** DSX Hadoop Integration Service (DSXHI) is a service that can be installed on a Hadoop edge node to allow DSX Local Version 1.2 or later clusters to securely access data residing on the Hadoop cluster, submit interactive Spark jobs, build models, and schedule jobs that run as a YARN application on the Hadoop cluster.

This repo contains 8 jupyter notebooks and 6 scripts that are used to illustarte the three scenarios mentioned above.

![](doc/source/images/Jupyter-notebooks-list.png)

**Scenario 1 (Run pyspark model in remote HDP cluster)**: <br>
- Spam Filter on local spark.ipynb (pyspark in local) <br>
- Spam Filter on remote spark.ipynb (pyspark in HDP) <br>

**Scenario 2 (Run scikit-learn model in remote HDP cluster)**: <br>
- Spam Filter using Scikit learn on local spark.ipynb (python env in local) <br>
- Spam Filter using Scikit learn on remote spark.ipynb (python env in HDP) <br>

**Scenario 3 (Deploy pyspark and scikit-learn model as egg in remote HDP cluster)**: <br>
- Building the Spam Filter Egg.ipynb, SpamFilter using egg deploy on remote Spark.ipynb (pyspark egg in HDP) <br>
- Building the Spam Filter Scikit Egg.ipynb, SpamFilter Scikit using egg deploy on remote Spark.ipynb (python egg in HDP) <br>

![](doc/source/images/scripts-list.png)

When you have completed this code pattern, you will understand how to:

* Load data into Spark DataFrames and use Spark's machine learning library (MLlib) to develop, train and deploy the Spam Filter model
* Load the data into pandas DataFrames and use Scikit-learn machine learning lbrary to develop, train and deploy the spam Filter model
* Use sparkmagics library to connect to the remote spark service in HDP cluter through DSXHI
* Use sparkmagics library to push the python virtual environment containing the Scikit-learn library to the remote HDP cluster through DSXHI
* Package the spam filter model as a python egg and distribute the egg to the remote HDP cluster through DSXHI 
* Run the spam filter model (both pyspark and scikit-learn versions) in remote HDP cluster utilizing the remote spark context and remote python virtual environment all within IBM's DSX Local

## Flow

![](doc/source/images/architecture.png)

**Scenario 1**:
1. Load the spam collection dataset using spark context in DSX Local
2. Use Spark Data Pipeline to extract the TF-IDF features and use Spark MLlib to train the spam filter pyspark model locally
3. Push the dataset to the remote HDFS user directory in the HDP cluster
4. Connect to remote spark context in HDP cluster through DSXHI using sparkmagics library
5. Use %%spark to run the steps 1 and 2 which now uses remote spark context to load, extarct and train the spam filter pyspark model in HDP cluster

**Scenario 2**:
1. Load the spam collection dataset using pandas in DSX Local
2. Use scikit-learn libraries to extract the Bag of Words features and to train the spam filter python model locally
3. Push the dataset to the remote HDFS user directory in the HDP cluster
4. Connect to remote spark context in HDP cluster through DSXHI using sparkmagics library
5. Push the python virtual environment loaded with scikit-learn to HDP cluster through DSXHI using sparkmagics library
6. Use %%spark to run the steps 1 and 2 which now uses remote python compute environment to load, extarct and train the spam filter python model in HDP cluster

**Scenario 3**:
1. Build the Spam filter pyspark model as an egg using the scripts
2. Build the Spam filter python model as an egg using the scripts
3. Push the dataset to the remote HDFS user directory in the HDP cluster
4. Push the pyspark egg and python(scikit) egg to the remote HDFS user directory in the HDP cluster
5. Connect to remote spark context in HDP cluster through DSXHI using sparkmagics library
6. Push the python virtual environment loaded with scikit-learn to HDP cluster through DSXHI using sparkmagics library
7. Use %%spark to deploy the pyspark egg and python egg to the remote HDP cluster
5. Use %%spark to run the functions provided by pyspark and python eggs to train the spam filter model in HDP cluster

## Included components

* [IBM Data Science Experience Local](https://content-dsxlocal.mybluemix.net/docs/content/local/overview.html): An out-of-the-box on premises solution for data scientists and data engineers. It offers a suite of data science tools that integrate with RStudio, Spark, Jupyter, and Zeppelin notebook technologies.
* [Apache Spark](http://spark.apache.org/): An open-source, fast and general-purpose cluster computing system.
* [Hortonworks Data Platform (HDP)](https://hortonworks.com/products/data-platforms/hdp/): HDP is a massively scalable platform for storing, processing and analyzing large volumes of data. HDP consists of the essential set of Apache Hadoop projects including MapReduce, Hadoop Distributed File System (HDFS), HCatalog, Pig, Hive, HBase, Zookeeper and Ambari.
* [Apache Livy](https://livy.incubator.apache.org/): Apache Livy is a service that enables easy interaction with a Spark cluster over a REST interface.
* [Jupyter Notebooks](http://jupyter.org/): An open-source web application that allows you to create and share documents that contain live code, equations, visualizations and explanatory text.

## Featured technologies

* [Artificial Intelligence](https://medium.com/ibm-data-science-experience): Artificial intelligence can be applied to disparate solution spaces to deliver disruptive technologies.
* [Python](https://www.python.org/): Python is a programming language that lets you work more quickly and integrate your systems more effectively.

# Prerequisites

## Access to HDP Platform

The core of this code pattern is integrating Hortonworks Data Platform (HDP) and IBM DSX Local. If you do not already have an HDP cluster available for use, you will need to install one before attempting to complete the code pattern. 

To install [HDP v2.6.4](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.4/index.html), please follow the [installation guide](https://docs.hortonworks.com/HDPDocuments/Ambari-2.6.1.5/bk_ambari-installation/content/ch_Getting_Ready.html) provided by Hortonworks. It first requires the installation of the [Apache Ambari](https://ambari.apache.org/) management platform which is then used to faciliate the HDP cluster installation. The Ambari Server is also required to complete a number of steps described in the following sections.

> Note: Ensure that your Ambari Server is configured to use `Python v2.7`.

## Install HDP Cluster services

Once your HDP cluster is deployed, at a minimum, install the following services as listed in this Ambari Server UI screenshot:

![](doc/source/images/ambari-services.png)

> Note: This code pattern requires that version `2.2.0` of the `Spark2` service be installed.

## Install DSX Local

https://content-dsxlocal.mybluemix.net/docs/content/local/welcome.html

## Install DSX Hadoop Integration Service (DSXHI) with HDP

https://content-dsxlocal.mybluemix.net/docs/content/local/hdp.html#hdp__option-1

## Register DSX Hadoop Integration Service (DSXHI) with DSXL

https://content-dsxlocal.mybluemix.net/docs/content/local/hadoopintegration.html

# Steps

Follow these steps to setup the proper environment to run our notebooks locally.

1. [Clone the repo](#1-clone-the-repo)
1. [Create project in IBM DSX Local](#2-create-project-in-ibm-dsx-local)
1. [Create project assets](#3-create-project-assets)
1. [Run the notebooks listed in each scenario to train and deploy the Spam Filter Model](#4-run-the-notebooks-listed-in-each-scenario-to-train-and-deploy-the-spam-filter-model)
1. [Commit changes to DSX Local Master Repository](#5-commit-changes-to-dsx-local-master-repository)


### 1. Clone the repo
```
git clone https://github.com/IBM/sms-spam-filter-using-hortonworks.git
```
### 2. Create project in IBM DSX Local

In DSX Local, we use projects as a container for all of our related assets. To create a project:

* From the DSX Local home page, select the `Add Project` button.

![](doc/source/images/dsx-local-project-list.png)

* Enter your project name and press the `Create` button.

![](doc/source/images/dsx-local-create-project.png)

### 3. Create project assets

Once created, you can view all of the project assets by selecting the `Assets` tab from the project's home page.

![](doc/source/images/dsx-local-notebook-list.png)

For our project, we need to add our notebooks and scripts. To add our notebooks:

* Select `Notebooks` in the project `Assets` list, then press the `Add Notebook` button.

* Enter a unique notebook name and use the `From URL` option to load the notebook from the github repo.

![](doc/source/images/dsx-local-create-notebook-1.png)

* Enter this URL:

```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-features.ipynb
```

* Repeat this step to add the remaining 7 notebooks, using the following URLs:
```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-modeling.ipynb
```
```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-modeling.ipynb
```
```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-modeling.ipynb
```
```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-modeling.ipynb
```
```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-modeling.ipynb
```
```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-modeling.ipynb
```
```
https://raw.githubusercontent.com/IBM/model-mgmt-on-dsx-local-and-hortonworks/master/notebooks/pca-modeling.ipynb
```

To add our scripts:

* Select `Scripts` in the project `Assets` list, then press the `Add Script` button.

![](doc/source/images/dsx-local-scripts-list.png)

* Enter a unique script name and click on the `From File` tab. Use the `Drag and Drop` option to load the script file from your local repo.

![](doc/source/images/dsx-local-create-script.png)

* Add the following scripts:
```
scripts/
scripts/
scripts/
scripts/
scripts/
scripts/
```

### 4. Run the notebooks listed in each scenario to train and deploy the Spam Filter Model

To view our notebooks, Select `Notebooks` in the project `Assets` list.

![](doc/source/images/Jupyter-notebooks-list.png)

First, some background on how to execute a notebook: 

> When a notebook is executed, what is actually happening is that each code cell in
the notebook is executed, in order, from top to bottom.
>
> Each code cell is selectable and is preceded by a tag in the left margin. The tag
format is `In [x]:`. Depending on the state of the notebook, the `x` can be:
>
>* A blank, this indicates that the cell has never been executed.
>* A number, this number represents the relative order this code step was executed.
>* A `*`, which indicates that the cell is currently executing.
>
>There are several ways to execute the code cells in your notebook:
>
>* One cell at a time.
>   * Select the cell, and then press the `Play` button in the toolbar.
>* Batch mode, in sequential order.
>   * From the `Cell` menu bar, there are several options available. For example, you
    can `Run All` cells in your notebook, or you can `Run All Below`, that will
    start executing from the first cell under the currently selected cell, and then
    continue executing all cells that follow.
>* At a scheduled time.
>   * Press the `Schedule` button located in the top right section of your notebook
    panel. Here you can schedule your notebook to be executed once at some future
    time, or repeatedly at your specified interval.

For specific details regarding each cell please follow through the steps provided below.

### 5. Commit changes to DSX Local Master Repository

After making changes to your project, you will be occasionally reminded to commit and push your changes to the DSX Local Master Repoisory.

![](doc/source/images/dsx-local-commit-request.png)

Now that we have added our notebooks and scripts, and generated our model, let's go ahead and do that. Commit and push all of our new assets, and set the version tag to `v1.0`.

![](doc/source/images/dsx-local-push-project.png)

# Troubleshooting

* An error was encountered: Session XX unexpectedly reached final status 'dead'. See logs: java.lang.Exception: No YARN application is found with tag livy-session-XX in 120 seconds. Please check your cluster status, it is may be very busy.

If you see this error trying to start a remote Spark session (which is attempted in the pca-features notebook), it may indicate that the username that you logged into DSX Local with has not been registered on the HDP Hadoop cluster.

# Links

* [Teaming on Data: IBM and Hortonworks Broaden Relationship](https://hortonworks.com/blog/teaming-data-ibm-hortonworks-broaden-relationship/)
* [Certification of IBM Data Science Experience (DSX) on HDP is a Win-Win for Customers](https://hortonworks.com/blog/certification-ibm-data-science-experience-dsx-hdp-win-win-customers/)
* [An Exciting Data Science Experience on HDP](https://hortonworks.com/blog/exciting-data-science-experience-hdp/)

# Learn more

* **Data Analytics Code Patterns**: Enjoyed this Code Pattern? Check out our other [Data Analytics Code Patterns](https://developer.ibm.com/code/technologies/data-science/)
* **AI and Data Code Pattern Playlist**: Bookmark our [playlist](https://www.youtube.com/playlist?list=PLzUbsvIyrNfknNewObx5N7uGZ5FKH0Fde) with all of our Code Pattern videos
* **Watson Studio**: Master the art of data science with IBM's [Watson Studio](https://datascience.ibm.com/)
* **Spark on IBM Cloud**: Need a Spark cluster? Create up to 30 Spark executors on IBM Cloud with our [Spark service](https://console.bluemix.net/catalog/services/apache-spark)

# License
[Apache 2.0](LICENSE)
