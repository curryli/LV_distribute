
/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{

  var community = -1L
  var communitySigmaTot = 0L
  var internalWeight = 0L  // self edges
  var nodeWeight = 0L;  //out degree
  var changed = false
   
  override def toString(): String = {
    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
    ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
  }
  
  // graph.vertices.saveAsTextFile(outputdir+"/level_"+level+"_vertices") 保存的 每一行是  (4,{community:1,communitySigmaTot:11,internalWeight:0,nodeWeight:4})
  //如果不重写toString函数，则保存为(4,VertexState@578333b)这种样子

}