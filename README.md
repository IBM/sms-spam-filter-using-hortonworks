# Discover, Train and Deploy Spam Filter Model on Hortonworks Data Platform using DSX Local

The goal of this code pattern is to demonstrate how data scientists can leverage remote spark clusters and compute environments from Hortonworks Data Platform (HDP) to train and deploy a Spam Filter Model using IBM's Data Science Experience Local (DSX Local).

A Spam Filter is a classification model built using natural language processing and machine learning algorithms. The model is trained on the [sms spam collection dataset](https://www.kaggle.com/ishansoni/sms-spam-collection-dataset) to classify whether a given text message is spam, or ham (not spam).

This code pattern provides three different examples or illustrations to tackle this problem:

Note: All the development and training included below, including references to the HDP cluster, are initiated from within IBM DSX Local.

* Develop and train a Spam Filter Model using pyspark, both locally (using local spark ML provided by DSX Local) and remotely (by leveraing the remote spark in the HDP cluster).

* Develop and train a Spam Filter Model using the 3rd-party library Scikit-learn, both locally (using Scikit-learn provided by DSX Local) and remotely (by leveraging the compute in the HDP cluster).

* Package the Spam Filter Model as a python egg in DSX Local, then train and deploy the model package levaraging both the remote spark and compute in the HDP cluster.

In order for DSX Local to be able to utilize the resources (both spark and compute) of the HDP cluster, the IBM DSX Hadoop Integration Service (DSXHI) must be installed on the edge node of the HDP cluster and DSXHI must be registered with DSX Local.

First, some background:

> **What is HDP?** Hortonworks Data Platform (HDP) is a massively scalable platform for storing, processing and analyzing large volumes of data. HDP consists of the essential set of Apache Hadoop projects including MapReduce, Hadoop Distributed File System (HDFS), HCatalog, Pig, Hive, HBase, Zookeeper and Ambari.

  ![](doc/source/images/hdp_arch.png)

   *Hortonworks Data Platform by [Hortonworks](https://hortonworks.com/products/data-platforms/hdp/)*

> **What is IBM DSX Local?** DSX Local is an on premises solution for data scientists and data engineers. It offers a suite of data science tools that integrate with RStudio, Spark, Jupyter, and Zeppelin notebook technologies. And yes, it can be configured to use HDP, too.

> **What is the IBM DSXHI?** DSX Hadoop Integration Service (DSXHI) is a service that can be installed on a Hadoop edge node to allow DSX Local (version 1.2 or later) clusters to securely access data residing on the Hadoop cluster, submit interactive Spark jobs, build models, and schedule jobs that run as a YARN application on the Hadoop cluster.

This code pattern contains 8 jupyter notebooks and 6 scripts. Here is a view of the notebooks as shown by the DSX Local UI:

![](doc/source/images/Jupyter-notebooks-list.png)

As mentioned earlier, this code pattern offers three examples of how to develope, train, and deploy a Spam Filter Model. For each example, multiple notebooks will be provided to show both a local (DSX Local) and remote (HDP cluster) solution. The following lists the associated notebooks for each:

* Develop and train a Spam Filter using pyspark.

  `"Spam Filter on local spark"` uses local spark ML.<br>
  `"Spam Filter on remote spark"` uses the remote spark in the HDP cluster.

* Develop and train a Spam Filter using the 3rd-party library Scikit-learn.

  `"Spam Filter using Scikit learn on local spark"` uses local Scikit-learn.<br>
  `"Spam Filter using Scikit learn on remote spark"` uses the compute in the HDP cluster.

* Package a Spam Filter model as a python egg, then train and deploy the model package remotely in the HDP cluster.

  `"Building the Spam Filter Egg"` uses local spark ML to build and train the model.<br>
  `"SpamFilter using egg deploy on remote Spark"` deploys and runs the previously built spark ML model leveraging both the remote spark and compute in the HDP cluster.<br>
  <br>
  `"Building the Spam Filter Scikit Egg"` uses local Scikit-learn to build and train the model.<br>
  `"SpamFilter Scikit using egg deploy on remote Spark"` deploys and runs the previously built Scikit-learn model leveraging both the remote spark and compute in the HDP cluster.

When you have completed this code pattern, you will understand how to:

* Load data into Spark DataFrames and use Spark's machine learning library (MLlib) to develop, train and deploy the Spam Filter Model.
* Load the data into pandas DataFrames and use Scikit-learn machine learning lbrary to develop, train and deploy the Spam Filter Model.
* Use the `sparkmagics` library to connect to the remote spark service in the HDP cluster via DSXHI.
* Use the `sparkmagics` library to push the python virtual environment containing the Scikit-learn library to the remote HDP cluster via DSXHI.
* Package the Spam Filter model as a python egg and distribute the egg to the remote HDP cluster via DSXHI.
* Run the Spam Filter Model (both pyspark and Scikit-learn versions) in the remote HDP cluster utilizing the remote spark context and the remote python virtual environment, all from within IBM DSX Local.

## Flow

![](doc/source/images/architecture.png)

**Example 1**:
1. Load the spam collection dataset using spark context in DSX Local
2. Use Spark Data Pipeline to extract the TF-IDF features and use Spark MLlib to train the spam filter pyspark model locally
3. Push the dataset to the remote HDFS user directory in the HDP cluster
4. Connect to remote spark context in HDP cluster via DSXHI using sparkmagics library
5. Use %%spark to run the steps 1 and 2 which now uses remote spark context to load, extract and train the spam filter pyspark model in the HDP cluster

**Example 2**:
1. Load the spam collection dataset using pandas in DSX Local
2. Use scikit-learn libraries to extract the `Bag of Words` features and to train the spam filter python model locally
3. Push the dataset to the remote HDFS user directory in the HDP cluster
4. Connect to remote spark context in HDP cluster via DSXHI using sparkmagics library
5. Push the python virtual environment loaded with scikit-learn to the HDP cluster via DSXHI using sparkmagics library
6. Use %%spark to run the steps 1 and 2 which now uses remote python compute environment to load, extarct and train the spam filter python model in the HDP cluster

**Example 3**:
1. Build the Spam filter pyspark model as an egg using the scripts
2. Build the Spam filter python model as an egg using the scripts
3. Push the dataset to the remote HDFS user directory in the HDP cluster
4. Push the pyspark egg and python(scikit) egg to the remote HDFS user directory in the HDP cluster
5. Connect to remote spark context in HDP cluster via DSXHI using sparkmagics library
6. Push the python virtual environment loaded with scikit-learn to the HDP cluster via DSXHI using sparkmagics library
7. Use %%spark to deploy the pyspark egg and python egg to the remote HDP cluster
5. Use %%spark to run the functions provided by pyspark and python eggs to train the spam filter model in the HDP cluster

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

To install [HDP v2.6.4](https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.4/index.html), please follow the [installation guide](https://docs.hortonworks.com/HDPDocuments/Ambari-2.6.1.5/bk_ambari-installation/content/ch_Getting_Ready.html) provided by Hortonworks. It first requires the installation of the [Apache Ambari](https://ambari.apache.org/) management platform which is then used to faciliate the HDP cluster installation.

> Note: Ensure that your Ambari Server is configured to use `Python v2.7`.

## Install HDP Cluster services

Once your HDP cluster is deployed, at a minimum, install the following services as listed in this Ambari Server UI screenshot:

![](doc/source/images/ambari-services.png)

> Note: This code pattern requires that version `2.2.0` of the `Spark2` service be installed.

## Install DSX Local

https://content-dsxlocal.mybluemix.net/docs/content/local/welcome.html provides links for installation and administration/user guides.

## Install DSX Hadoop Integration Service (DSXHI) with HDP

https://content-dsxlocal.mybluemix.net/docs/content/local/hdp.html#hdp__option-1

## Register DSX Hadoop Integration Service (DSXHI) with DSXL

https://content-dsxlocal.mybluemix.net/docs/content/local/hadoopintegration.html

# Steps

Follow these steps to setup the proper environment to run our notebooks locally.

1. [Clone the repo](#1-clone-the-repo)
1. [Create project in IBM DSX Local](#2-create-project-in-ibm-dsx-local)
1. [Create project assets](#3-create-project-assets)
1. [Commit changes to DSX Local Master Repository](#4-commit-changes-to-dsx-local-master-repository)
1. [Run the notebooks listed for each example](#5-run-the-notebooks-listed-for-each-example)

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

For our project, we need to add our notebooks, scripts, and data sets. To add our notebooks:

* Select `Notebooks` in the project `Assets` list, then press the `Add Notebook` button.

* Enter a unique notebook name and use the `From URL` option to load the notebook from the github repo.

![](doc/source/images/dsx-local-create-notebook-3.png)

* Enter this URL:

```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/Spam%20Filter%20on%20local%20spark.jupyter.ipynb
```

> Note: while you are free to use any notebook name you wish, it may be easier to follow along with these instructions if you copy the names listed previously, which is basically lifting the name directly from the URL. For example, name the previous notebook `"Spam Filter on local spark"`.

* Repeat this step to add the remaining 7 notebooks, using the following URLs:
```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/Spam%20Filter%20on%20remote%20spark.jupyter.ipynb
```
```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/Spam%20Filter%20using%20Scikit%20learn%20on%20local%20spark.jupyter.ipynb
```
```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/Spam%20Filter%20using%20Scikit%20learn%20on%20remote%20spark.jupyter.ipynb
```
```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/Building%20the%20Spam%20Filter%20Egg.jupyter.ipynb
```
```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/Building%20the%20Spam%20Filter%20Scikit%20Egg.jupyter.ipynb
```
```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/SpamFilter%20Scikit%20using%20egg%20deploy%20on%20remote%20Spark.jupyter.ipynb
```
```
https://raw.githubusercontent.com/IBM/sms-spam-filter-using-hortonworks/master/notebooks/SpamFilter%20using%20egg%20deploy%20on%20remote%20Spark.jupyter.ipynb
```

To add our scripts:

* Select `Scripts` in the project `Assets` list, then press the `Add Data Set` button.

![](doc/source/images/dsx-local-scripts-list.png)

* Click on the `From File` tab. Use the `Drag and Drop` option to load the script file from your local repo. For script `Name`, keep the default value that is auto-generated from the downloaded file.

![](doc/source/images/dsx-local-create-script.png)

* Add the following scripts:
```
scripts/DataPipeline.py
scripts/LRModel.py
scripts/LRModelScikit.py
scripts/__init__.py
scripts/setup.py
scripts/setup2.py
```

To add our data set:

* Select `Data sets` in the project `Assets` list, then press the `Add Script` button.

![](doc/source/images/dsx-local-create-data-set.png)

* Click the `Select from your local file system` button to select the file `/data/SMSSpamCollection.csv` from your local repo.

### 4. Commit changes to DSX Local Master Repository

After making changes to your project, you will be occasionally reminded to commit and push your changes to the DSX Local Master Repoisory.

![](doc/source/images/dsx-local-commit-request.png)

Now that we have added our assets, let's go ahead and do that. Commit and push all of our new assets, and set the version tag to `v1.0`.

![](doc/source/images/dsx-local-push-project.png)

### 5. Run the notebooks listed for each example

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

As described above, this code pattern provides 3 examples of solving the Spam Filter problem. And each of the examples consists of multiple notebooks. We recommend that you run all of them to see the multiple ways to build, train and deploy a model, and for  accessing resources on the remote HDP cluster.

While each of the notebooks is well documented, the are some actions that will be covered in more detail below.

#### 1. Upload data to remote HDP cluster

To upload data from the DSX Local cluster to the HDP cluster, utilize the `upload_hdfs_file` method from `dsx_core_utils` library. This is only available when DSXHI is registered to upload the dataset to the remote HDP cluster.

![](doc/source/images/Upload-data-remote-cluster.png)

> Note: set both the target path and the HDFS Web URL to match your environment

#### 2. Connect to remote spark on the HDP cluster through DSXHI via sparkmagics library

To create a remove Spark session on your HDP cluster, utilize the `sparkmagics` and `dsx_core_utils` library.

As shown below, copy the Livy endpoint associated with your HDP cluster, then add it as a DSXHI endpoint.

![](doc/source/images/Connect-remote-cluster-dsxhi-add-endpoint.png)

Then create a remote session as shown below.

![](doc/source/images/Connect-remote-cluster-dsxhi-create-session.png)

> Note: Be patient - it may take a few minutes.

#### 3. Run Spam Filter Pyspark Model in HDP cluster using %% within DSX Local

With the remote spark session created, use %%spark as a notation which will run the cell contents in remote spark service in the HDP cluster.

Note: If you use %spark - it will run the cell contents in local spark in DSX Local

![](doc/source/images/Executing-in-remote-spark-context.png)

Run the spam filter pyspark model in remote HDP cluster using %%spark in the beginning of the cell.

![](doc/source/images/Spark-ML-Model.png)

#### 4. Push python virtual environment to remote HDP cluster through DSXHI via sparkmagics library

HDP cluster doesn't natively support third party libraries such as scikit-learn, Keras, Tensor flow etc. 

In order to run the spam filter python model built using the scikit-learn library, the python virtual environment used in DSX Local needs to be pushed to the remote HDP cluster as shown below.

![](doc/source/images/Push-python-virtual-environment.png)

Copy the DSXHI connection properties containing the python virtual environment to the `properties` tab in `%manage_spark` window and create the remote session.

![](doc/source/images/Push-python-virtual-environment2.png)

#### 5. Run Spam Filter python Model in HDP cluster using %% within DSX Local

With the remote python session created, use %%spark as a notation which will run the cell contents in remote python environemnt in HDP cluster.

![](doc/source/images/Python-ML-Model1.png)

Run the spam filter python scikit-learn model in the remote HDP cluster using %%spark in the beginning of the cell.

![](doc/source/images/Python-ML-Model2.png)

#### 6. Build Spam Filter Pyspark egg and execute the LRModel in remote HDP cluster

Another approach to running the spam filter model is to package the code that is run via notebooks in previous examples into a egg file and then distribute the egg file across the remote HDP cluster.

Once distributed, the model function can be invoked via DSX Local to execute the spam filter model.

After copying the necessary scripts, run the cell below to build the spam filter pyspark egg.

![](doc/source/images/Build-spam-filter-egg.png)

Connect to remote spark in HDP cluster and run the LRModel function using %%spark as notation to execute the spam filter pyspark model.

![](doc/source/images/Run-spam-filter-egg.png)

#### 7. Build Spam Filter python egg and execute the LRModelScikit in remote HDP cluster

After copying the necessary scripts, run the cell below to build the spam filter scikit-learn egg.

![](doc/source/images/Build-scikit-egg.png)

Connect to remote spark in HDP cluster and run the LRModelScikit function using %%spark as notation to execute the spam filter scikit-learn model.

![](doc/source/images/Run-scikit-egg.png)

# Troubleshooting

* An error was encountered: Session XX unexpectedly reached final status 'dead'. See logs: java.lang.Exception: No YARN application is found with tag livy-session-XX in 120 seconds. Please check your cluster status, it is may be very busy.

If you see this error trying to start a remote Spark session, it may indicate that the username that you logged into DSX Local with has not been registered on the HDP Hadoop cluster.

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
