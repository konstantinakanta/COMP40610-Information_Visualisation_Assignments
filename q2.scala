import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.EdgeDirection


case class Patent(fromId :Long, toId :Long)
def parsePatent(str: String): Patent = {
val line = str.split("\t")
Patent(line(0).toLong, line(1).toLong)
}

var dataRDD = sc.textFile("/cit-Patent.txt")
val header = dataRDD.first()
dataRDD = dataRDD.filter(row => row != header)
val patentRDD = dataRDD.map(parsePatent).cache()
val vertices = dataRDD.map(_.split("\t")).cache().cache().flatMap(array => array).distinct().map(x => (x.toLong, "nowhere"))
vertices.take(2)
val nowhere = "nowhere"
val citedEdges = patentRDD.map(pat => ((pat.fromId, pat.toId),"")).distinct
citedEdges.take(2)
val edges = citedEdges.map {case ((fromId, toId),"") =>Edge(fromId,toId,"") }
edges.take(1)
val graph = Graph(vertices, edges, nowhere)
val oneHops = graph.collectNeighborIds(EdgeDirection.In)
oneHops.take(2)
val flat = oneHops.flatMap(x => x._2.map(y => (y,x._1)))
val twoHops : RDD[(VertexId, Array[VertexId])] = flat.join(oneHops).map(x => (x._2._1, x._2._2)).reduceByKey(_ ++ _)
twoHops.take(2)
val hopsCombined = twoHops.join(oneHops)
hopsCombined.map(x => (x._1,x._2._1 ++ x._2._2)).map(y => (y._1, y._2.distinct.length)).sortBy(x=> x._2, false).take(10).foreach(println)