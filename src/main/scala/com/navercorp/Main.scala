package com.navercorp

import java.io.Serializable

import com.navercorp.graph.GraphOps
import com.navercorp.lib.AbstractParams
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object Main {

  object Command extends Enumeration {
    type Command = Value
    val node2vec, randomwalk, embedding, sample = Value
  }

  import Command._

  case class Params(iter: Int = 10,
                    lr: Double = 0.025,
                    numPartition: Int = 10,
                    dim: Int = 128,
                    window: Int = 10,
                    walkLength: Int = 80,
                    numWalks: Int = 10,
                    p: Double = 1.0,
                    q: Double = 1.0,
                    weighted: Boolean = true,
                    directed: Boolean = false, //无向图
                    degree: Int = 30,
                    indexed: Boolean = true,
                    nodePath: String = null,
                    input: String = null, /*"/user/hadoop-nlpml/chenlongzhen/test/nodeQueryDataGen_2019-04-05_2019-05-05",*/
                    output: String = null,
                    /*"/user/hadoop-airec/chenlongzhen/node2vec/randwalk/nodeQueryDataGen_2019-04-05_2019-05-05_randwalk_test",*/
                    cmd: Command = Command.randomwalk
                   ) extends AbstractParams[Params] with Serializable

  val defaultParams = Params()

  val parser = new OptionParser[Params]("Node2Vec_Spark") {
    head("Main")
    opt[Int]("walkLength")
      .text(s"walkLength: ${defaultParams.walkLength}")
      .action((x, c) => c.copy(walkLength = x))
    opt[Int]("window")
      .text(s"window: ${defaultParams.window}")
      .action((x, c) => c.copy(window = x))
    opt[Double]("lr")
      .text(s"learning rate: ${defaultParams.lr}")
      .action((x, c) => c.copy(lr = x))
    opt[Int]("dim")
      .text(s"emb dim: ${defaultParams.dim}")
      .action((x, c) => c.copy(dim = x))
    opt[Int]("iter")
      .text(s"iter: ${defaultParams.iter}")
      .action((x, c) => c.copy(iter = x))
    opt[Int]("numPartition")
      .text(s"numPartition: ${defaultParams.numPartition}")
      .action((x, c) => c.copy(numPartition = x))

    opt[Int]("walkLength")
      .text(s"walkLength: ${defaultParams.walkLength}")
      .action((x, c) => c.copy(walkLength = x))
    opt[Int]("numWalks")
      .text(s"numWalks: ${defaultParams.numWalks}")
      .action((x, c) => c.copy(numWalks = x))
    opt[Double]("p")
      .text(s"return parameter p: ${defaultParams.p}")
      .action((x, c) => c.copy(p = x))
    opt[Double]("q")
      .text(s"in-out parameter q: ${defaultParams.q}")
      .action((x, c) => c.copy(q = x))
    opt[Boolean]("weighted")
      .text(s"weighted: ${defaultParams.weighted}")
      .action((x, c) => c.copy(weighted = x))
    opt[Boolean]("directed")
      .text(s"directed: ${defaultParams.directed}")
      .action((x, c) => c.copy(directed = x))
    opt[Int]("degree")
      .text(s"degree: ${defaultParams.degree}")
      .action((x, c) => c.copy(degree = x))
    opt[Boolean]("indexed")
      .text(s"Whether nodes are indexed or not: ${defaultParams.indexed}")
      .action((x, c) => c.copy(indexed = x))
    opt[String]("input")
      .required()
      .text("Input file path: empty")
      .action((x, c) => c.copy(input = x))
    opt[String]("output")
      .required()
      .text("Output path: empty")
      .action((x, c) => c.copy(output = x))
    opt[String]("nodePath")
      .text("Output node2index file path: empty")
      .action((x, c) => c.copy(nodePath = x))
    opt[String]("cmd")
      .required()
      .text(s"command: ${defaultParams.cmd.toString}")
      .action((x, c) => c.copy(cmd = Command.withName(x)))
    note(
      """
        |For example, the following command runs this app on a synthetic dataset:
        |
        | bin/spark-submit --class com.nhn.sunny.vegapunk.ml.model.Node2vec \
      """.stripMargin +
        s"|   --lr ${defaultParams.lr}" +
        s"|   --iter ${defaultParams.iter}" +
        s"|   --numPartition ${defaultParams.numPartition}" +
        s"|   --dim ${defaultParams.dim}" +
        s"|   --window ${defaultParams.window}" +
        s"|   --input <path>" +
        s"|   --node <nodeFilePath>" +
        s"|   --output <path>"
    )
  }

  def main(args: Array[String]) = {
    parser.parse(args, defaultParams).map { param =>
      val conf = new SparkConf()
      val context: SparkContext = new SparkContext(conf)
      context.setCheckpointDir("viewfs:///user/hadoop-airec/zhangxiaohan05/node2vec/randwalk/checkpoint")

      GraphOps.setup(context, param)
      Node2vec.setup(context, param)
      Word2vec.setup(context, param)
      Sample.setup(context, param)

      param.cmd match {
        case Command.node2vec =>
          val graph = Node2vec.loadGraph()
          val randomPaths: RDD[String] = Node2vec.randomWalk(graph)
          Node2vec.save(randomPaths)
          Word2vec.readFromRdd(randomPaths).fit().save()

        case Command.randomwalk =>
          val graph = Node2vec.loadGraph()
          val randomPaths: RDD[String] = Node2vec.randomWalk(graph)
          Node2vec.save(randomPaths)

        case Command.embedding => {
          val randomPaths = Word2vec.read(param.input)
          Word2vec.fit().save()
        }

        case Command.sample => {
          val samples = Sample.read()
          Sample.save(samples)
        }
      }
    } getOrElse {
      sys.exit(1)


    }
  }
}
