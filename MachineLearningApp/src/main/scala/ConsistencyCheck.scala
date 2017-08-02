/* ConsistencyCheck.scala */

import java.io.File //necessary for file handling
import java.text.SimpleDateFormat //data formatting
import org.rogach.scallop._ //command line parsing library
import com.typesafe.config._ //loading configuration from configuration file
//import ch.cern.sparkmeasure.StageMetrics // sparkmeasure

object ConsistencyCheckApp {
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
      
    // determine application name according to parameter file
    val appName = configuration.getString("ml.app.name") //retrieve predefined string
    println(dateFormatter.format(new java.util.Date()) + " CHECK Application name: " + appName)
    
    val featuresFileName = configuration.getString("ml.files.featuresFile") // extract the relevant information
    val featuresFile = new File(featuresFileName)
    if (featuresFile.exists) {
      println(dateFormatter.format(new java.util.Date()) + " CHECK Existing features file: " + featuresFileName)  
    } else {
      println(dateFormatter.format(new java.util.Date()) + " MISSING Features file: " + featuresFileName)
      
    }
    
    val labelsFileName = configuration.getString("ml.files.labelsFile")
    val labelsFile = new File(labelsFileName)
    if (labelsFile.exists) {
      println(dateFormatter.format(new java.util.Date()) + " CHECK Existing labels file: " + labelsFileName)
    } else {
      println(dateFormatter.format(new java.util.Date()) + " MISSING Labels file: " + labelsFileName)
    }
    
    val splitsFileName = configuration.getString("ml.files.splitsFile")
    val splitsFile = new File(splitsFileName)
    if (splitsFile.exists) {
      println(dateFormatter.format(new java.util.Date()) + " CHECK Existing splits file: " + splitsFileName)
    } else {
      println(dateFormatter.format(new java.util.Date()) + " MISSING Splits file: " + splitsFileName)
    }    
  }
}
