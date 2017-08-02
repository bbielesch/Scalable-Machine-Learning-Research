/* GenreLogReg.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//import all necessary libraries for random forest machine learning
//Spark ML is the data-frame based new machine learning library, Spark MLlib is the old one (currently in maintenance mode)
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{ LogisticRegression, LogisticRegressionModel }
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.ml.tuning.{ ParamGridBuilder, CrossValidator }

//all other imports
import java.io.File //necessary for file handling
import java.text.SimpleDateFormat //data formatting
import org.rogach.scallop._ //command line parsing library
import com.typesafe.config._ //loading configuration from configuration file
import collection.JavaConverters._ //converting between java collections and scala collections
//import ch.cern.sparkmeasure.StageMetrics // sparkmeasure

object GenreLogRegCrossValidateApp {
  def main(args: Array[String]) {

    class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
      val config = opt[File](required = true)
      validateFileExists(config)
      verify()
    }

    val dateFormatter = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
    val commandlineparams = new Conf(args)
    val configFilePath = commandlineparams.config.apply().getAbsolutePath()
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Using configuration file: " + configFilePath)
    
    val configuration = ConfigFactory // read the configuration from file
      .parseFile(commandlineparams.config.apply())
      .resolve() //substitute environment variables
    val featuresFile = configuration.getString("ml.files.featuresFile") // extract the relevant information
    val labelsFile = configuration.getString("ml.files.labelsFile")
    val splitsFile = configuration.getString("ml.files.splitsFile")
    
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Using Features file: " + featuresFile)
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Using Labels file: " + labelsFile)
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Using Splits file: " + splitsFile)
    
    //all parameters for logistic regression
    val maxIter: Array[Int] = configuration
      .getIntList("ml.params.maxIter")
      .asScala.toArray.map(_.toInt)
    println(dateFormatter.format(new java.util.Date()) 
        + " CONFIG Linear Regression: Max number of iterations: " 
        + maxIter.mkString(" "))
    
    val regParam: Array[Double] = configuration
      .getDoubleList("ml.params.regParam")
      .asScala.toArray.map(_.toDouble)
    println(dateFormatter.format(new java.util.Date()) 
        + " CONFIG Linear Regression: Regularization parameter: " 
        + regParam.mkString(" "))
    
    val elasticNetParam: Array[Double] = configuration
      .getDoubleList("ml.params.elasticNetParam")
      .asScala.toArray.map(_.toDouble)
    println(dateFormatter.format(new java.util.Date()) 
        + " CONFIG Linear Regression: ElasticNet mixing paramete: " 
        + elasticNetParam.mkString(" "))

    // Folds for cross validation
    val numFolds = 
      if (configuration.hasPath("ml.params.numFolds")) 
        configuration.getInt("ml.params.numFolds") else 10  //this is no array - would not make sense
    println(dateFormatter.format(new java.util.Date()) 
        + " CONFIG Random Forest: Cross Validator: Number of Folds: " + numFolds)
    
    var taskStartTime = System.nanoTime() //measure execution time of complete task
    
    val conf = new SparkConf() //used for supplying configuration settings via command line
    
    // if supplied extract configuration parameters
    val executorsInstances = 
      if(conf.contains("spark.executors.instances"))
        " ExecInst" + conf.get("spark.executors.instances") else ""
    val executorCores = 
      if(conf.contains("spark.executor.cores"))
        " ExecCores" + conf.get("spark.executor.cores") else ""
    val executorMemory = 
      if(conf.contains("spark.executor.memory"))
        " ExecMem" + conf.get("spark.executor.memory") else ""
    val coresMax =
      if (conf.contains("spark.cores.max"))
        " CoresMax" + conf.get("spark.cores.max") else ""
    
    // determine correct master connect string
    /*
    val master = 
      if(conf.contains("spark.master")) //if command line contains spark.master
        conf.get("spark.master") else  //then use this
        configuration.getString("ml.app.master") //otherwise use spark.master from config file
    conf.setMaster(master)
    */
          
    val master = conf.get("spark.master")
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Connect to master: " + master)
    
    //determine correct application name
    var appName = configuration.getString("ml.app.name") //retrieve predefined string
    appName = appName + executorsInstances + coresMax + executorCores + executorMemory // extend by additional information
    conf.setAppName(appName) //and set it
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Application name: " + appName) //and finally print it
    
    val spark = SparkSession.builder() 
      .config(conf) //use configuration options defined above
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //this is the features table
    val features = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") // default false as it requires two passes through the data
      .option("mode", "DROPMALFORMED")
      .load(featuresFile)

    //this is the labels table  
    val labelNames = Seq("MSD_TRACKID", "GENRE")
    val labels = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", "\t")
      .load(labelsFile)
      .toDF(labelNames: _*)

    //this is the splits table
    val splitsNames = Seq("MSD_TRACKID", "SUBSET")
    val splits = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", "\t")
      .option("comment", "%")
      .load(splitsFile)
      .toDF(splitsNames: _*)

    //join all three tables and drop the MSD_TRACKID column
    val mlDataRaw = splits.join(labels, Seq("MSD_TRACKID"))
      .join(features, Seq("MSD_TRACKID"))
      .drop("MSD_TRACKID")

    //utilize vector assembler to get the data frame in the correct shape
    val va = new VectorAssembler().setOutputCol("features")
      .setInputCols(mlDataRaw.columns.diff(Array("GENRE", "SUBSET")))
    val mlData = va.transform(mlDataRaw).select("features", "GENRE", "SUBSET")
      .withColumnRenamed("GENRE", "label")

    //generate train and test data               
    val trainData = mlData.filter($"SUBSET" === "TRAIN")
      .drop("MSD_TRACKID").drop("SUBSET")
    println(dateFormatter.format(new java.util.Date()) + " RESULT Records in Training Dataset: " + trainData.count())
    val testData = mlData.filter($"SUBSET" === "TEST")
      .drop("MSD_TRACKID").drop("SUBSET")
    println(dateFormatter.format(new java.util.Date()) + " RESULT Records in Testing Dataset: " + testData.count())
    
      
    //######## Define functions for ml pipeline #######
    
    // Index labels, adding metadata to the label column
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(mlData) //fit whole dataset not just training data

    // Automatically identify categorical features, and index them
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(mlData)

    // Define a RandomForest model
    val logReg = new LogisticRegression()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, logReg, labelConverter))

    //######## Define cross validation #########
      
    val paramGrid = new ParamGridBuilder()
      .addGrid(logReg.maxIter, maxIter)
      .addGrid(logReg.regParam, regParam)
      .addGrid(logReg.elasticNetParam, elasticNetParam)
      .build()
    
    val evaluatorCrossValidation = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
      
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluatorCrossValidation)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)
      
    //######## Execution of machine learning tasks #######
      
    //Train model. This also runs the indexers
    val crossValidatorModel = cv.fit(trainData)

    println(dateFormatter.format(new java.util.Date()) + " RESULT Grid search results:")

    crossValidatorModel
      .getEstimatorParamMaps
      .zip(crossValidatorModel.avgMetrics)
      .foreach(println)

    println(dateFormatter.format(new java.util.Date()) + 
        " RESULT Best set of parameters found:" + 
        crossValidatorModel
          .getEstimatorParamMaps
          .zip(crossValidatorModel.avgMetrics)
          .maxBy(_._2)
          ._1)
    
    // perform predictions for testing data
    val predictions = crossValidatorModel.transform(testData)
    
    // Select example rows to display.
    println(dateFormatter.format(new java.util.Date()) + " RESULT Sample Predictions & Ground Truth Comparison" )
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(dateFormatter.format(new java.util.Date()) + " RESULT Test Error = " + (1.0 - accuracy))

    var taskDuration = (System.nanoTime - taskStartTime) / 1e9d
    println(dateFormatter.format(new java.util.Date()) + " RESULT Duration of complete task " + taskDuration.toString())
  }
}
