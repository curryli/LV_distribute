import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


/**
 * Execute the louvain distributed community detection.
 * Requires an edge file and output directory in hdfs (local files for local mode only)
 */
object Main {
  
  def main(args: Array[String]) {
    
    var edgeFile = "xrli/graphx/Zakhary.csv"   //空手道数据集
    var outputdir = "xrli/graphx/Zakharyout"
    var edgedelimiter = "\t"
    var parallelism = -1
    var minProgress  = 2000 
    var progressCounter  = 4

    
   
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("Louvain") 
    val sc = new SparkContext(conf)
    
    
    // read the input into a distributed edge list
    var edgeRDD = sc.textFile(edgeFile).map(row=> {
	      val tokens = row.split(edgedelimiter).map(_.trim())
	      tokens.length match {
	        case 2 => {new Edge(tokens(0).toLong , tokens(1).toLong , 1L) }
	        case 3 => {new Edge(tokens(0).toLong , tokens(1).toLong, tokens(2).toLong)}
	        case _ => {throw new IllegalArgumentException("invalid input line: "+row)}
	      }
	   })	   
	
	// if the parallelism option was set map the input to the correct number of partitions,
	// otherwise parallelism will be based off number of HDFS blocks
	if (parallelism != -1 ) edgeRDD = edgeRDD.coalesce(parallelism,shuffle=true)
  
    // create the graph
    val graph = Graph.fromEdges(edgeRDD, None)
  
    // use a helper class to execute the louvain
    // algorithm and save the output.
    // to change the outputs you can extend LouvainRunner.scala
    val runner = new HDFSLouvainRunner(minProgress,progressCounter,outputdir)
    runner.run(sc, graph)
    

  }
  
}



