
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.Array.canBuildFrom
import java.io._
 

/**
 * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 * 
 * See LouvainHarness for algorithm details
 */
class HDFSLouvainRunner(minProgress:Int,progressCounter:Int,outputdir:String) extends LouvainHarness(minProgress:Int,progressCounter:Int){

  var qValues = Array[(Int,Double)]()
      
   //Graph[VertexState,Long]表示节点属性是VertexState类型，边属性是Long类型
  override def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
	  var verticePath = outputdir+"/level_"+level+"_vertices"
    var edgePath = outputdir+"/level_"+level+"_edges"
    graph.vertices.saveAsTextFile(verticePath)
    graph.edges.saveAsTextFile(edgePath)
      //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
      //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")  
    qValues = qValues :+ ((level,q))
    println(s"qValue: $q")
        
      // overwrite the q values at each level
      sc.parallelize(qValues, 1).saveAsTextFile(outputdir+"/level_"+level+"_qvalues")
      
//////////////////////////////////////保存最终的社团包括的最初所有原始的节点号////////////////////////////////////////////////////////////////
      
    val VInfoRDD = sc.textFile(verticePath)      //(5,{community:5,communitySigmaTot:6,internalWeight:0,nodeWeight:3})
    
    var pairRDD = VInfoRDD.map{ line =>
	     var effectline = line.split("\\(")(1)         //5,{community:5,communitySigmaTot:6,internalWeight:0,nodeWeight:3}
	     var InfoList = effectline.split(",\\{")
       var Vid = InfoList(0).trim()
	     var community = InfoList(1).split(",")(0).split(":")(1).trim()
	     (community,Vid)
	  }
	  
    if(level == 0){ 
	    pairRDD.reduceByKey((x,y)=>x.toString() + " " + y.toString()).saveAsTextFile(outputdir+"/level_"+level+"_communitys")
    }
    else{
      var FormerVerticePath = outputdir+"/level_"+ (level-1) +"_communitys"
      val FormerVInfoRDD = sc.textFile(FormerVerticePath)      //(5,{community:5,communitySigmaTot:6,internalWeight:0,nodeWeight:3})
      var FormerpairRDD = FormerVInfoRDD.map{ line =>
	      var effectline = line.split("\\(")(1)         //5,{community:5,communitySigmaTot:6,internalWeight:0,nodeWeight:3}
	      var InfoList = effectline.split(",")
        var Vid = InfoList(0).trim()
	      var communityList = InfoList(1).trim()
	      (Vid,communityList)
	    }

      val valueList = pairRDD.values.collect()
      var cpair: scala.collection.mutable.Map[String, String] =  scala.collection.mutable.Map()
      
     
      for(pair <- pairRDD.collect() ){
        if(!cpair.contains(pair._1 ))
          if(pair._1== pair._1)
            cpair += (pair._1 -> (FormerpairRDD.lookup(pair._2).mkString(" ")))
          else  
            cpair += (pair._1 -> (FormerpairRDD.lookup(pair._1).mkString(" ") + " " + FormerpairRDD.lookup(pair._2).mkString(" ")))
            
        else
          if(pair._1== pair._1)
             cpair(pair._1) = cpair(pair._1) +  FormerpairRDD.lookup(pair._2).mkString(" ")
          else  
             cpair(pair._1) = cpair(pair._1) +  FormerpairRDD.lookup(pair._1).mkString(" ") + " " + FormerpairRDD.lookup(pair._2).mkString(" ")
      }
    
//      println("cpair")
//      cpair.foreach(println)
  
     var pairList = cpair.toArray
     
     var pairArray = pairList.map( item => item._1 + "," + item._2.split("[^\\d]").mkString(" ") )
      
     var result = sc.parallelize(pairArray)

     result.saveAsTextFile(outputdir+"/level_"+level+"_communitys")
      
      
    }
     
	  
  }
  
  
  
  
  
  
  
  
}