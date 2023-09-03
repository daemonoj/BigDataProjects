import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {
  val rows = 100
  val columns = 100

  case class Pair (i: Int, j: Int){
    
    override
    def toString(): String = {
      var s = ""+i+"\t"+j
      s
    }
  }

  case class Triple (i: Int, j: Int, value: Double){

    override
    def toString(): String = {
      var s = ""+i+"\t"+"j"+"\t"+value
      s
    }
  }
  case class Block ( data: Array[Double] ) {
    
    override
    def toString (): String = {
      var s = "\n"
      for ( i <- 0 until rows ) {
        for ( j <- 0 until columns )
          s += "\t%.3f".format(data(i*rows+j))
        s += "\n"
      }
      s
    }
  }

  /* Convert a list of triples (i,j,v) into a Block */
  def toBlock ( triples: List[(Int,Int,Double)] ): Block = {
    var b = Block(Array.fill[Double](rows*columns)(0.0))
    for (l <- 0 to (triples.length - 1)){
      var i : Int = triples(l)._1
      var j : Int = triples(l)._2
      var value: Double = triples(l)._3
      b.data(i*rows + j) = value
    }
    b
  }

  /* Add two Blocks */
  def blockAdd ( m: Block, n: Block ): Block = {
    /* ... */
    var newB = Block(Array.fill[Double](rows*columns)(0.0))
    for (i <- 0 until rows){
      for (j <- 0 until columns){
        newB.data(i*rows+j) = m.data(i*rows + j) + n.data(i*rows + j)
      }
    }
    newB
  }

  /* Read a sparse matrix from a file and convert it to a block matrix */
  // 
  def createBlockMatrix ( sc: SparkContext, file: String ): RDD[((Int,Int),Block)] = {
    /* ... */
    println("inside function")
    val line = sc.textFile(file).map(f=> {val a = f.split(",")
      (a(0).toInt, a(1).toInt, a(2).toDouble)})
    // println("output: ", line.collect)
    val pair_triple = line.map{ case(i,j,value)=> ((i/100, j/100),(i%100, j%100, value))}
    val ret = pair_triple.groupByKey().map{ case((i,j), z)=> ((i,j), toBlock(z.toList))}

    // val values = line.map(f=>{
    //       val x = f.split(",")
    //       (x(0).toInt, x(1).toInt, x(2).toDouble)})
    // println(values)
    // val x = values.map{case(i, j, value)=> ((i/100, j/100),(i%100, j%100, value))}
    // val ret = x.groupByKey().collect
    // println(ret)
    // // ret
    // var s: Int = 10
    ret
  }

  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Add")
    val sc = new SparkContext(conf)
    
    // val M = sc.textFile(args(0))
    // .map( line => { val a = line.split (",")
    // ((a(0). toInt ,a(1). toInt ),a(2).toDouble)} )
    // val N = sc.textFile (args(1))
    // .map( line => { val a = line.split (",")
    // ((a(0). toInt ,a(1). toInt ),a(2).toDouble)} )
    // val res = M.map{case ((i,j),m) => ((i,j),m)}
    //   .join( N.map{case ((i,j),n) => ((i,j),n)} )
    //   .map{ case ((i,j),(m,n)) => ((i,j),m+n)}
    //   .reduceByKey(_+_)
    val mat1 = createBlockMatrix(sc, args(0))
    // println("gen: ", mat1)
    // mat1.saveAsTextFile(args(2))
    val mat2 = createBlockMatrix(sc, args(1))
    // mat2.saveAsTextFile(args(2))

    val res = mat1.join(mat2)
        .map{case ((i,j),(b1,b2))=>((i,j),blockAdd(b1,b2))}
    // res.collect().foreach(println)
        // .reduceByKey((b1,b2)=>blockAdd(b1,b2))
    res.saveAsTextFile(args(2))
    sc.stop()
  }
}
