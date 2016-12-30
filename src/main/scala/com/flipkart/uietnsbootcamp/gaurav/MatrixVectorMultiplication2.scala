package com.flipkart.uietnsbootcamp.gaurav

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by gaurav.malhotra on 29/12/16.
  */
object MatrixVectorMultiplication2 {
  def main(args: Array[String]): Unit= {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local")
    val sc = new SparkContext(conf)

    val matrix = sc.textFile("src/main/resources/MatrixSample.txt").map(line => line)
      .zipWithIndex.flatMap(a => a._1.split(",").toList.zipWithIndex.map(b => (a._2.toLong, b._2.toLong, b._1.toDouble)))
      .map(tuple => (tuple._2, (tuple._1, tuple._3)))


    val vector = sc.textFile("src/main/resources/VectorSample.txt")
      .map(line => (line.split(",")(0).toLong, line.split(",")(1).toDouble))
      .map(tuple => (tuple._1, tuple._2))

    println(matrix.collect().toList)
    println(vector.collect().toList)

    val joined = matrix.join(vector)
                 .map(x=>(x._2._1._1, x._2._1._2 * x._2._2))
                 .reduceByKey(_+_)

    println(joined.collect().toList)

  }
}
