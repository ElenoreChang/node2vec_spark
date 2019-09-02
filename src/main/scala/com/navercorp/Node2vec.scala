package com.navercorp

import java.io.Serializable

import com.navercorp.common.Property
import com.navercorp.graph.{EdgeAttr, GraphOps, NodeAttr}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object Node2vec extends Serializable {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  var context: SparkContext = _
  var config: Main.Params = _
  var label2id: RDD[(String, Long)] = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param

    this
  }

  def loadGraph() = {
    val bcDirected = context.broadcast(config.directed)
    val bcWeighted = context.broadcast(config.weighted)
    val inputTriplets = context.textFile(config.input).flatMap { triplet =>
      val parts = triplet.split("\\s")
      val weight = bcWeighted.value match {
        case true => Try(parts.last.toDouble).getOrElse(1.0)
        case false => 1.0
      }

      val (src, dst) = (parts.head, parts(1))
      if (bcDirected.value) {
        Array((src, dst, weight))
      } else {
        Array((src, dst, weight), (dst, src, weight))
      }
    }

    val triplets = config.indexed match {
      case true => inputTriplets.map { case (src, dst, weight) => (src.toLong, dst.toLong, weight) }
      case false =>
        val (label2id_, indexedTriplets) = indexingNode(inputTriplets)
        this.label2id = label2id_
        indexedTriplets
    }

    val bcMaxDegree = context.broadcast(config.degree) //出度上限
    val node2attr = triplets.map { case (src, dst, weight) =>
      (src, Array((dst, weight)))
    }.reduceByKey(_ ++ _).map { case (srcId, neighbors: Array[(Long, Double)]) =>
      var neighbors_ : Array[(Long, Double)] = neighbors.groupBy(_._1).map { case (group, traversable) =>
        traversable.head //去重
      }.toArray
      if (neighbors_.length > bcMaxDegree.value) { //将neighbors按照权重倒排，按照出度上限limit
        neighbors_ = neighbors.sortWith { case (left, right) => left._2 > right._2 }.slice(0, bcMaxDegree.value)
      }

      (srcId, NodeAttr(neighbors = neighbors_))
    }.repartition(1000).persist(StorageLevel.MEMORY_ONLY_SER) //, walk 失败时会重重复此步骤所以 cache 一下

    val edge2attr = node2attr.flatMap { case (srcId, clickNode) =>
      clickNode.neighbors.map { case (dstId, weight) =>
        Edge(srcId, dstId, EdgeAttr())
      }
    }.repartition(1000).persist(StorageLevel.MEMORY_ONLY_SER)

    GraphOps.initTransitionProb(node2attr, edge2attr)
  }

  def randomWalk(g: Graph[NodeAttr, EdgeAttr]) = {
    g.persist(StorageLevel.MEMORY_ONLY_SER)
    val edge2attr: RDD[(String, EdgeAttr)] = g.triplets.map { edgeTriplet =>
      (s"${edgeTriplet.srcId}${edgeTriplet.dstId}", edgeTriplet.attr)
    }.reduceByKey { case (l, r) => l }.partitionBy(new HashPartitioner(1000))
    edge2attr.checkpoint()
    edge2attr.persist(StorageLevel.MEMORY_ONLY_SER)

    //logger.info(s"edge2attr: ${edge2attr.count}")

    //val examples = g.vertices.cache
    //fix npe, add:.filter(x=>x._2.path.nonEmpty).
    val examples = g.vertices.filter(x => x._2.path.length >= 2)// line 106: 至少有俩个 vectice
    //logger.info(s"examples: ${examples.count}")

    g.unpersist(blocking = false)
    g.edges.unpersist(blocking = false)
    g.vertices.unpersist(blocking = false)

    var totalRandomPath: RDD[String] = null
    for (iter <- 0 until config.numWalks) {
      var prevRandomPath: RDD[String] = null
      var randomPath: RDD[String] = examples.map { case (nodeId, clickNode) =>
        clickNode.path.mkString("\t")
      }.persist(StorageLevel.MEMORY_ONLY_SER)
      var activeWalks = randomPath.first //此操作可避免 outofbounds?! 否则, 下面的循环在dag 中并行执行
      for (walkCount <- 0 until config.walkLength) {
        prevRandomPath = randomPath
        randomPath = edge2attr.join(randomPath.map{ iter =>
          //iter.map { pathBuffer =>
            val paths = iter.split("\t")
            (paths.slice(paths.size - 2, paths.size).mkString(""), iter)
         // }
        }).mapPartitions { iter => //speed up?
          /*iter.map { case (edge, (attr, pathBuffer)) =>
            try {
              if (pathBuffer != null && pathBuffer.nonEmpty) {
                val nextNodeIndex = GraphOps.drawAlias(attr.J, attr.q)
                val nextNodeId = attr.dstNeighbors(nextNodeIndex)

                s"$pathBuffer\t$nextNodeId"
              } else {
                null
              }
            } catch {
              case e: Exception => throw new RuntimeException(e.getMessage)
            }
          }*/
          // add the condition: attr.dstNeighbors != null && attr.dstNeighbors.nonEmpty
          iter.map { case (edge, (attr, pathBuffer)) =>
            try {
              if (pathBuffer != null /*&& pathBuffer.nonEmpty*/ && attr.dstNeighbors != null && attr.dstNeighbors.nonEmpty) {
                val nextNodeIndex = GraphOps.drawAlias(attr.J, attr.q)
                val nextNodeId = attr.dstNeighbors(nextNodeIndex)

                s"$pathBuffer\t$nextNodeId"
              } else {
                pathBuffer //add
              }
            } catch {
              case e: Exception => throw new RuntimeException(e.getMessage)
            }
          }.filter(_ != null)
        }.persist(StorageLevel.MEMORY_ONLY_SER)

        activeWalks = randomPath.first
        prevRandomPath.unpersist(blocking = false)
      }

      if (totalRandomPath != null) {
        val prevRandomWalkPaths = totalRandomPath
        totalRandomPath = totalRandomPath.union(randomPath)
        // totalRandomPath.count
        prevRandomWalkPaths.unpersist(blocking = false)
      } else {
        totalRandomPath = randomPath
      }
    }

    totalRandomPath
  }

  def indexingNode(triplets: RDD[(String, String, Double)]) = {
    val label2id = createNode2Id(triplets)

    val indexedTriplets = triplets.map { case (src, dst, weight) =>
      (src, (dst, weight))
    }.join(label2id).map { case (src, (edge: (String, Double), srcIndex: Long)) =>
      try {
        val (dst: String, weight: Double) = edge
        (dst, (srcIndex, weight))
      } catch {
        case e: Exception => null
      }
    }.filter(_ != null).join(label2id).map { case (dst, (edge: (Long, Double), dstIndex: Long)) =>
      try {
        val (srcIndex, weight) = edge
        (srcIndex, dstIndex, weight)
      } catch {
        case e: Exception => null
      }
    }.filter(_ != null)

    (label2id, indexedTriplets)
  }

  def createNode2Id[T <: Any](triplets: RDD[(String, String, T)]): RDD[(String, Long)] = triplets.flatMap { case (src, dst, weight) =>
    Try(Array(src, dst)).getOrElse(Array.empty[String])
  }.distinct().zipWithIndex()

  def save(randomPaths: RDD[String]): this.type = {
    randomPaths.filter(x => x != null && x.replaceAll("\\s", "").length > 0)
      .repartition(1000)
      .saveAsTextFile(s"${config.output}.${Property.pathSuffix}")

    if(!config.indexed) { //如果输入不是indexed，则输出 label2id 到 nodePath
      this.label2id
        .map{case (node: String, index: Long) => s"$node\t$index"}
        .repartition(100)
        .saveAsTextFile(s"${config.nodePath}.${Property.node2idSuffix}")
    }

//    if (Some(this.label2id).isDefined) {
//      label2id.map { case (label, id) =>
//        s"$label\t$id"
//      }.saveAsTextFile(s"${config.output}.${Property.node2idSuffix}")
//    }

    this
  }

}
