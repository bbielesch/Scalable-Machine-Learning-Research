/* LinerRegression.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//import all necessary libraries for random forest machine learning
//Spark ML is the data-frame based new machine learning library, Spark MLlib is the old one (currently in maintenance mode)
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }

//all other imports
import java.io.File //necessary for file handling
import java.text.SimpleDateFormat //data formatting
import org.rogach.scallop._ //command line parsing library
import com.typesafe.config._ //loading configuration from config file
//import ch.cern.sparkmeasure.StageMetrics // sparkmeasure

object YearLinearRegressionApp {
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
    
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Using Features file: " + featuresFile)
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Using Labels file: " + labelsFile)
    
    val appName = configuration.getString("ml.app.name")
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Application name: " + appName)
     
    //all parameters for linear regression
    val maxIter = 
       if(configuration.hasPath("ml.params.maxIter"))
           configuration.getInt("ml.params.maxIter") else 10
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Linear Regression: Max number of iterations: " + maxIter)
    
    val regParam = 
       if (configuration.hasPath("ml.params.regParam")) 
         configuration.getDouble("ml.params.regParam") else 0.3
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Linear Regression: Maximum tree depth: " + regParam)
    
    val elasticNetParam = 
      if (configuration.hasPath("ml.params.elasticNetParam")) 
        configuration.getDouble("ml.params.elasticNetParam") else 0.8
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Linear Regression: Feature subset strategy: " + elasticNetParam)  
    
    val trainSplit = 
      if (configuration.hasPath("ml.params.trainSplit")) 
        configuration.getDouble("ml.params.trainSplit") else 0.7
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Linear Regression: Train/Test Split: " + trainSplit)

    var taskStartTime = System.nanoTime() //measure execution time of complete task
    
    val conf = new SparkConf()

    val master = conf.get("spark.master")
    println(dateFormatter.format(new java.util.Date()) + " CONFIG Connect to master: " + master)
    
    // if supplied extract configuration parameters
    val executorsInstances = 
      if(conf.contains("spark.executors.instances"))
        " ExecInstances " + conf.get("spark.executors.instances") else ""
    val executorCores = 
      if(conf.contains("spark.executor.cores"))
        " ExecCores " + conf.get("spark.executor.cores") else ""
    val executorMemory = 
      if(conf.contains("spark.executor.memory"))
        " ExecMem " + conf.get("spark.executor.memory") else ""
    
    //use application name from config file but extend if necessary
    conf.setAppName(appName + executorsInstances + executorCores + executorMemory) 
    
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
    val labels = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true") // default false as it requires two passes through the data
      .option("mode", "DROPMALFORMED")
      .load(labelsFile)

    //join both tables and drop surplus columns
    val mlDataRaw = labels.join(features, Seq("MSD_TRACKID"))
      .drop("MSD_TRACKID")
      .drop("artist") // both columns not really necessary for ML
      .drop("song")   

    //utilize vector assembler to get the data frame in the correct shape
    val va = new VectorAssembler().setOutputCol("features")
      .setInputCols(mlDataRaw.columns.diff(Array("year")))
    val mlData = va.transform(mlDataRaw)
      .select("features", "year")
      .withColumnRenamed("year", "label")
    
    // Split the data into training and test sets (30% held out for testing).
    val Array(trainData, testData) = mlData.randomSplit(Array(trainSplit, 1 - trainSplit))
    
    val lr = new LinearRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)

    // Fit the model
    val lrModel = lr.fit(trainData)

    // Print the coefficients and intercept for linear regression
    println(dateFormatter.format(new java.util.Date()) + s" RESULT Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(dateFormatter.format(new java.util.Date()) + s" RESULT NumIterations: ${trainingSummary.totalIterations}")
    println(dateFormatter.format(new java.util.Date()) + s" RESULT ObjectiveHistory: [${trainingSummary.objectiveHistory.mkString(", ")}]")
    
    // trainingSummary.residuals.show() // not necessary
    println(dateFormatter.format(new java.util.Date()) + s" RESULT Root Mean Squared Error (RMSE): ${trainingSummary.rootMeanSquaredError}")
    println(dateFormatter.format(new java.util.Date()) + s" RESULT r2: ${trainingSummary.r2}")
    
    // based on https://www.analyticsvidhya.com/blog/2017/01/scala/
    
    // Build up predictions for testing data
    val testDataPredict = lrModel.transform(testData)
    
    // Select example rows to display.
    // testDataPredict.select("prediction", "label", "features").show(5) // not necessary

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
      
    val rmse = evaluator.evaluate(testDataPredict)
    println(dateFormatter.format(new java.util.Date()) + " RESULT Root Mean Squared Error (RMSE) on test data: " + rmse)
    
    var taskDuration = (System.nanoTime - taskStartTime) / 1e9d
    println(dateFormatter.format(new java.util.Date()) + " FINISH Duration of complete task " + taskDuration.toString())
  }
}
