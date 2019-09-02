package com.navercorp

import com.navercorp.Main.Params
import com.navercorp.common.Property
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Sample {
  var context: SparkContext = _
  var config: Params = _

  def setup(context: SparkContext, param: Main.Params): this.type = {
    this.context = context
    this.config = param

    this
  }

  def sampleWordContext(path: List[String]): List[(String, String)] = {
    val head = path.take(path.size - 1)
    val tail = path.takeRight(path.size - 1)
    head.zip(tail)
  }

  def read(): RDD[String] = {
    val samples = context
      .textFile(config.input)
      .repartition(200)
      .map(_.split("\t").toList)
      .filter(_.size > 1)
      .map { path: List[String] => sampleWordContext(path) }
      .flatMap(e => e)
      .map { case (word: String, context: String) => s"""$word\t$context""" }

    samples
  }

  def save(samples: RDD[String]): this.type = {
    samples
      .saveAsTextFile(s"${config.output}.${Property.sampleSuffix}")

    this
  }

}
