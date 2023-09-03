import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf)

    // A graph is a dataset of vertices, where each vertex is a triple
    //   (group,id,adj) where id is the vertex id, group is the group id
    //   (initially equal to id), and adj is the list of outgoing neighbors
    val default_value : Long = -1
    var graph: RDD[ ( Long, Long, List[Long] ) ]
       = /* put your code here */      
        // read the graph from the file args(0)
        sc.textFile(args(0)).map(f=> {
          val a = f.split(",")
          val id = a(0).toLong
          val neighbours = a.drop(1).map(_.toLong).toList
          (id, id, neighbours)
        })

    for ( i <- 1 to 5 ) {
       // For each vertex (group,id,adj) generate the candidate (id,group)
       //    and for each x in adj generate the candidate (x,group).
       // Then for each vertex, its new group number is the minimum candidate
      val groups: RDD[ ( Long, Long ) ]=  
        
        graph.map{ case(gid, id, adj)=>{
          val candidateList = (id, gid) :: adj.map((_, gid))
          candidateList
        }}.flatMap(f=>f).reduceByKey((a,b)=> if (a<b) a else b)
        // println("Printing Groups")
        // groups.foreach(println)
       // reconstruct the graph using the new group numbers
      val l = List(default_value)
      graph = groups.map{ case (id, gid) => (id, (gid, l))}
        .join(graph.map{case(group_id, vid, adj)=>(vid, (group_id, adj))})
        .map{case(vid,((gid, adj1),(gid2, adj2)))=>(gid, vid, adj2)} 
      // println("Printing Graphs")
      // graph.foreach(println)
      // q.foreach(println)
    }
       /* put your code here */

    // // print the group sizes
    val ret = 
      graph.map{ case (gid, id, adj)=> (gid, 1)}
      .reduceByKey(_+_)

    ret.collect().foreach(println)
    /* put your code here */

    sc.stop()
  }
}
