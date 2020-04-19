import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.EdgeDirection


case class Patents(fromId :Long, toId :Long)
def parsePatent(str: String): Patents = {
val line = str.split(",")
Patents(line(0).toLong, line(1).toLong)
}

var textRDD = sc.textFile("/cit-Patents.txt")
val header = textRDD.first()
textRDD = textRDD.filter(row => row != header)
val patentsRDD = textRDD.map(parsePatent).cache()

val patents_vertices = textRDD.map(_.split(",")).cache().cache().flatMap(array => array).distinct().map(x => (x.toLong, "nowhere"))
patents_vertices.take(2)
val nowhere = "nowhere"
val citEdges = patentsRDD.map(pat => ((pat.fromId, pat.toNodeId),"")).distinct
citEdges.take(2)
val edges = citEdges.map {case ((pat.fromId, pat.toNodeId),"") =>Edge(pat.fromId, pat.toNodeId,"") }
edges.take(1)

val graph = Graph(patents_vertices, edges, nowhere)

val oneHop = graph.collectNeighborIds(EdgeDirection.In)
oneHop.take(2)

val flat = oneHop.flatMap(x => x._2,map(y => (y,x._1)))
val twoHops : RDD[(VertexId, Array[VertexId])] = flat.join(oneHop).map(x => x._2._1. x._2._2.reduceByKey(_ ++ _)
twoHops.take(2)

val hopsCombined = twoHops.join(oneHop)
hopsCombined.map(x => (x._1,x._2._1 ++ x._2._2)).map(y => (y._1, y._2.distinct.length)).sortBy(x=> x._2, false).take(10).foreach(println)

case class Patents(fromId :Long, toId :Long)
def parsePatent(str: String): Patents = {
val line = str.split("\t")
Patents(line(0).toLong, line(1).toLong)
}

var textRDD = sc.textFile("/cit-Patents.txt")
val header = textRDD.first()
textRDD = textRDD.filter(row => row != header)
val patentsRDD = textRDD.map(parsePatent).cache()

val vertices = textRDD.map(_.split("\t")).cache().cache().flatMap(array => array).distinct().map(x => (x.toLong, "nowhere"))
vertices.take(2)
val nowhere = "nowhere"
val citEdges = patentsRDD.map(pat => ((pat.fromId, pat.toId),"")).distinct
citEdges.take(2)
val edges = citEdges.map {case ((fromId, toId),"") =>Edge(fromId,toId,"") }
edges.take(1)

val graph = Graph(vertices, edges, nowhere)

val oneHop = graph.collectNeighborIds(EdgeDirection.In)
oneHop.take(2)

val flat = oneHop.flatMap(x => x._2.map(y => (y,x._1)))
val twoHops : RDD[(VertexId, Array[VertexId])] = flat.join(oneHop).map(x => (x._2._1, x._2._2)).reduceByKey(_ ++ _)
twoHops.take(2)

val hopsCombined = twoHops.join(oneHop)
hopsCombined.map(x => (x._1,x._2._1 ++ x._2._2)).map(y => (y._1, y._2.distinct.length)).sortBy(x=> x._2, false).take(10).foreach(println)




