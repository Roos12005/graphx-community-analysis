package org.mazerunner.extension

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import sys.process._
import java.io.File
import org.mazerunner.core._ 


object ProcessManager{

	var tempPath = "tmp_Relation"
	var tempMergePath = "FullGraph.txt"
	//var edgeListPath: String = ""	

	def runTestData(sc: SparkContext, numPartition: Int, srcPath: String, algorithm: String, dstPath: String) = {
		tempMergePath = srcPath
		println("[Status] > GraphProcessing : Algorithms = " + algorithm + " ... ")
		processGraph(sc, numPartition, algorithm, dstPath)

	}

    def runHiveData(sc: SparkContext, numPartition: Int, sqlStm: String, algorithm: String, dstPath: String) = {		
		//Set resultPath
		//edgeListPath = dstPath

		var tableUse = sqlStm.toLowerCase().split("from")(1).split(" ")(1).trim

        //Get connect to database.table
        val sqlContext = new HiveContext(sc)
        val hiveTable: DataFrame = sqlContext.sql("SELECT *  FROM " + tableUse)

        println("[Status] > Getting DB Schema ... " + tableUse)

        //Show schema and example row
        hiveTable.printSchema()
        hiveTable.show()

        //Select data to build relation
        println("[Status] > Selecting ... " + sqlStm)
        val data = sqlContext.sql(sqlStm)
        data.show()
		
		//Tranfrom data into (key value) 
		println("[Status] > Mapping (Src_node Dst_node) ...")
		val tfOrder = data.map( o => o(0) + " " + o(1))
		//tfOrder.collect.foreach(println)

	       
		//Save data into dstPath
		println("[Status] > Saved "+ tempPath)
		HDFSFileService.removeFile(tempPath)
		HDFSFileService.removeFile(tempMergePath)
		tfOrder.saveAsTextFile(tempPath)

		//Do merge data part-0000[0-*] into single file
		println(tempMergePath)
		mergeResult(tempPath + "/*", tempMergePath)
		//mergeAndSplitFile(tempPath + "/*", edgeListPath, 500)

		println("[Status] > GraphProcessing : Algorithms = PR ... ")
		processGraph(sc, numPartition, algorithm, dstPath)
	}	

	def mergeResult(pathToMerge: String, dstPath: String){
		val cmd = Seq("/bin/sh", "-c", "hdfs dfs -cat " + new File(pathToMerge) + " | hdfs dfs -put -f - " + new File(dstPath)).!!
	}

	def mergeAndSplitFile(pathToMerge: String, dstPath: String, range: Long){
		val cmd = Seq("/bin/sh", "-c", "hdfs dfs -cat " + new File(pathToMerge) + " | split -l " + range + " - | hdfs dfs -put - " +  new File(dstPath)).!!
	}

	def processGraph(sc: SparkContext, numPartition: Int, al: String, dstPath: String){
		var result: java.lang.Iterable[String] = null
		al match {
			case "PR" => result = algorithms.pageRank(sc, tempMergePath) 
			case "CC" => result = algorithms.closenessCentrality(sc, numPartition, tempMergePath)
			case "BC" => result = algorithms.betweennessCentrality(sc, numPartition, tempMergePath)
			case "LC" => result = algorithms.louvainCommunityDetection(sc, tempMergePath)
			case "KBC" => result = algorithms.kBetweennessCentrality(sc, numPartition, tempMergePath)
			case "ALL" => {
				var cc = algorithms.closenessCentrality(sc, numPartition, tempMergePath)
				var kbc = algorithms.kBetweennessCentrality(sc, numPartition, tempMergePath)
				var lc = algorithms.louvainCommunityDetection(sc, tempMergePath)

				println("[Status] > GraphProcessing : FullGraph ... Done!")
				HDFSFileService.writeIterate("FullGraph-CC.txt", cc.iterator())
				HDFSFileService.writeIterate("FullGraph-KBC.txt", kbc.iterator())
				HDFSFileService.writeIterate("FullGraph-LC.txt", lc.iterator())

				var tempLCPath = "CommunityGraph.txt"
				mergeResult("output-LC/level_999_MapEdges/*", tempLCPath)
				cc = algorithms.closenessCentrality(sc, numPartition, tempLCPath)
				kbc = algorithms.kBetweennessCentrality(sc, numPartition, tempLCPath)
				println("[Status] > GraphProcessing : CommunityGraph ... Done!")
				HDFSFileService.writeIterate("CommunityGraph-CC.txt", cc.iterator())
				HDFSFileService.writeIterate("CommunityGraph-KBC.txt", kbc.iterator())

			}
		}

		if(al != "ALL"){
			//Iterate println
			var iter: java.util.Iterator[String] = result.iterator()
			//while(iter.hasNext()){
			//	print(iter.next())
			//}
			println("[Status] > GraphProcessing : Algorithms = " +  al + " ... Done!")
			HDFSFileService.writeIterate(dstPath, iter)
		}

		
	}
}
