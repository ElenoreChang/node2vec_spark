package com.navercorp.graph

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, _}
import org.apache.spark.rdd.RDD
import com.navercorp.Main
import org.apache.spark.storage.StorageLevel

object GraphOps {
  var context: SparkContext = _
  var config: Main.Params = _
  
  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param
    
    this
  }

  /**
    * input: vertice 对应的 node 及其权重,
    * @param nodeWeights 已按照 weights 降序排列
    * @return
    */
  def setupAlias(nodeWeights: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val K = nodeWeights.length //邻居节点个数
    val J = Array.fill(K)(0)
    val q = Array.fill(K)(0.0)

    val smaller = new ArrayBuffer[Int]()
    val larger = new ArrayBuffer[Int]()

    val sum = nodeWeights.map(_._2).sum //边权重之和
    nodeWeights.zipWithIndex.foreach { case ((nodeId, weight), i) => //编号，i 与 weights 反向
      q(i) = K * weight / sum //（当前边权重/边权重均值）
      if (q(i) < 1.0) { //当前边权重 < 均值
        smaller.append(i)
      } else { //当前边权重 >= 均值
        larger.append(i)
      }
    }

    while (smaller.nonEmpty && larger.nonEmpty) {
      val small = smaller.remove(smaller.length - 1)
      val large = larger.remove(larger.length - 1)

      J(small) = large
      q(large) = q(large) + q(small) - 1.0
      if (q(large) < 1.0) smaller.append(large)
      else larger.append(large)
    }

    (J, q)
  }

  def setupEdgeAlias(p: Double = 1.0, 
                     q: Double = 1.0)(srcId: Long,
                                      srcNeighbors: Array[(Long, Double)],
                                      dstNeighbors: Array[(Long, Double)]): (Array[Int], Array[Double]) = {
    val neighbors_ = dstNeighbors.map { case (dstNeighborId, weight) =>
      var unnormProb = weight / q
      if (srcId == dstNeighborId) unnormProb = weight / p
      else if (srcNeighbors.exists(_._1 == dstNeighborId)) unnormProb = weight

      (dstNeighborId, unnormProb)
    }

    setupAlias(neighbors_)
  }

  def drawAlias(J: Array[Int], q: Array[Double]): Int = {
    val K = J.length
    val kk = math.floor(math.random * K).toInt

    if (math.random < q(kk)) kk
    else J(kk)
  }

  def initTransitionProb(indexedNodes: RDD[(VertexId, NodeAttr)], indexedEdges: RDD[Edge[EdgeAttr]]) = {
    val bcP = context.broadcast(config.p)
    val bcQ = context.broadcast(config.q)
    
    /*val graph = Graph(indexedNodes, indexedEdges).mapVertices[NodeAttr] { case (vertexId, nodeAttr) =>
      val (j, q) = GraphOps.setupAlias(nodeAttr.neighbors)
      val nextNodeIndex = GraphOps.drawAlias(j, q)
      nodeAttr.path = Array(vertexId, nodeAttr.neighbors(nextNodeIndex)._1)
      nodeAttr
    }.mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
      val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId, 
        edgeTriplet.srcAttr.neighbors,
        edgeTriplet.dstAttr.neighbors)
      
      edgeTriplet.attr.J = j
      edgeTriplet.attr.q = q
      edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)
      
      edgeTriplet.attr
    }.cache*/

    //fix npe issue according to repo issue solution
    val graph = Graph(
      indexedNodes,
      indexedEdges,
      null.asInstanceOf[NodeAttr],
      StorageLevel.MEMORY_ONLY_SER,
      StorageLevel.MEMORY_ONLY_SER
    ).
      partitionBy(PartitionStrategy.RandomVertexCut).
      mapVertices[NodeAttr] { case (vertexId, nodeAttr) =>
      //确定 path
      if (nodeAttr != null) {  // add
        val (j, q) = GraphOps.setupAlias(nodeAttr.neighbors)
        val nextNodeIndex = GraphOps.drawAlias(j, q)
        nodeAttr.path = Array(vertexId, nodeAttr.neighbors(nextNodeIndex)._1)
        nodeAttr
      }else{
        NodeAttr() // create a new object
      }
    }.mapTriplets { edgeTriplet: EdgeTriplet[NodeAttr, EdgeAttr] =>
      val (j, q) = GraphOps.setupEdgeAlias(bcP.value, bcQ.value)(edgeTriplet.srcId,
        edgeTriplet.srcAttr.neighbors,
        edgeTriplet.dstAttr.neighbors)

      edgeTriplet.attr.J = j
      edgeTriplet.attr.q = q
      edgeTriplet.attr.dstNeighbors = edgeTriplet.dstAttr.neighbors.map(_._1)

      edgeTriplet.attr
    }

    graph
  }
  
}
