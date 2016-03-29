package org.mazerunner.extension

import org.apache.spark.{SparkContext, SparkConf}

import java.net.URLDecoder;
import java.io.UnsupportedEncodingException;

import java.util.Calendar



object SparkApplication{
	val CHECKPOINT_DIR: String = "tmpRDDCheckPoint/"
	var sparkContext: SparkContext = null
	var currentTime: Long = 0

	if(sparkContext == null){
		initializeSpark()
	}
	/*
	def SparkApplication(){
		if(sparkContext == null){
			initializeSpark()
		}

	}
	*/

	def initializeSpark(){
		var sparkConf = new SparkConf()
			.setAppName("SparkGraphProcessing")

		sparkContext = new SparkContext(sparkConf)
		var pathJar: String = this.getClass.getProtectionDomain().getCodeSource().getLocation().getPath()
		var deCodePath: String = ""
		try{
            deCodePath = URLDecoder.decode(pathJar, "UTF-8")
        }catch {
            case e: UnsupportedEncodingException => e.printStackTrace()
        }
        sparkContext.addJar(deCodePath)
        sparkContext.setCheckpointDir(CHECKPOINT_DIR)
	}

	def startApp(args: Array[String]){
		println(sparkContext)
		val mode = args(0)
		val numPartition: Int = args(1).toInt
		val sqlStm = args(2)
		val algorithm = args(3)
		val resultPath = args(4)
		currentTime = System.currentTimeMillis()
		println("[Status] > TimeStamp = " + currentTime)
		mode match{
			case "TEST" => ProcessManager.runTestData(sparkContext, numPartition, sqlStm, algorithm, resultPath)
			case "HIVE" => ProcessManager.runHiveData(sparkContext, numPartition, sqlStm, algorithm, resultPath)
		}
		
	}

	def main(args: Array[String]) {
		if(args.length!=5){
			println("Wrong Argument specify:")
			println("It should be 5 arguments")
			println("1. Mode")
			println("2. numPartiion")
			println("3. SQLStatment")
			println("4. Graph Algorithm")
			println("5. Result path")
		}else{
			println("[Status] > Starting ... ")
			startApp(args)
			println("[Status] > Done")
			println("[Status] > Processing Time = " + (System.currentTimeMillis() - currentTime)/ 1000 )	
		}
	}
}