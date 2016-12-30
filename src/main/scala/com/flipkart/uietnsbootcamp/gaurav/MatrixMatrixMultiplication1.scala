package com.flipkart.uietnsbootcamp.gaurav

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by gaurav.malhotra on 30/12/16.
  */
object MatrixMatrixMultiplication1 {

  def main(args: Array[String]): Unit= {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local")
    val sc = new SparkContext(conf)

    val matrix1 = sc.textFile("src/main/resources/MatrixSample.txt")
          .map(line => line).zipWithIndex.flatMap(a => a._1.split(",").toList.zipWithIndex.map(b => (a._2.toLong, b._2.toLong, b._1.toDouble)))
          .map(tuple => (tuple._2, (tuple._1, tuple._3)))

    val matrix2 = Source.fromFile("src/main/resources/MatrixSampleOther.txt").getLines.toList
          .map(line => line).zipWithIndex.flatMap(a => a._1.split(",").toList.zipWithIndex.map(b => (a._2.toLong, b._2.toLong, b._1.toDouble)))
          .map(tuple => (tuple._1, (tuple._2, tuple._3)))

    val globalMatrix2 = sc.broadcast(matrix2)

    val result = matrix1.flatMap(x =>{
      globalMatrix2.value.filter(t => t._1 == x._1).map(y =>{
        ((x._2._1, y._2._1), x._2._2*y._2._2)
      })
    }).reduceByKey(_+_).collect()

    println(result.toList)

  }
}
