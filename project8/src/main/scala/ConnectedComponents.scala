import org.apache.spark.graphx.{Graph,Edge,EdgeTriplet,VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ConnectedComponents {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Connected Components")
    val sc = new SparkContext(conf)

    // A graph G is a dataset of vertices, where each vertex is (id,adj),
    // where id is the vertex id and adj is the list of outgoing neighbors
    var G: RDD[ ( Long, List[Long] ) ]
    // read the graph from the file args(0)
       = sc.textFile(args(0)).map(f=> {
          val a = f.split(",")
          val id = a(0).toLong
          val neighbours = a.drop(1).map(_.toLong).toList
          (id, neighbours)
        })

    // graph edges have attribute values 0
    val edges: RDD[Edge[Int]] = G.map{case(source, neighbours) =>{
      neighbours.map(f=> Edge(source, f, 0))}}.flatMap(f=>f)
    // a vertex (id,group) has initial group equal to id
    val vertices: RDD[(Long,Long)] = G.map{case (src, neighbours) => {(src, src)}}

    // the GraphX graph
    val graph: Graph[Long,Int] = Graph(vertices,edges,0L)

    // find the vertex new group # from its current group # and the group # from the incoming neighbors
    def newValue ( id: VertexId, currentGroup: Long, incomingGroup: Long ): Long
      = math.min(currentGroup, incomingGroup)
        /* ... */


    // send the vertex group # to the outgoing neighbors
    def sendMessage ( triplet: EdgeTriplet[Long,Int]): Iterator[(VertexId,Long)]
      = Iterator((triplet.dstId, triplet.srcAttr))

    def mergeValues ( x: Long, y: Long ): Long
      = return math.min(x,y)

    // derive connected components using pregel
    val comps = graph.pregel (Long.MaxValue,5) (   // repeat 5 times
                      newValue,
                      sendMessage,
                      mergeValues
                   )

    // print the group sizes (sorted by group #)
    comps.vertices.map{ case (id, gid)=> (gid, 1)}.reduceByKey(_+_).sortBy(_._1).collect.foreach(println)
  }
}
