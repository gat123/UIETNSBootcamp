package com.flipkart.uietnsbootcamp.gaurav

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by gaurav.malhotra on 30/12/16.
  */
object MatrixMatrixMultiplication2 {

  def red(x:(Long, (Char, Long, Double)), y: (Long, (Char, Long, Double))) = {
    x
  }

  def main(args: Array[String]): Unit= {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local")
    val sc = new SparkContext(conf)

    val matrix1 = sc.textFile("src/main/resources/MatrixSample.txt")
          .map(line => line).zipWithIndex.flatMap(a => a._1.split(",").toList.zipWithIndex.map(b => (a._2.toLong, b._2.toLong, b._1.toDouble)))
          .map(tuple => (tuple._2, (tuple._1, tuple._3)))

    val matrix2 = sc.textFile("src/main/resources/MatrixSampleOther.txt")
          .map(line => line).zipWithIndex.flatMap(a => a._1.split(",").toList.zipWithIndex.map(b => (a._2.toLong, b._2.toLong, b._1.toDouble)))
          .map(tuple => (tuple._1, (tuple._2, tuple._3)))

    val result = matrix1.join(matrix2).map(x=>{
      ((x._2._1._1,x._2._2._1), x._2._1._2 * x._2._2._2)
    }).reduceByKey(_+_).collect()

    println(result.toList)

 //               .map(x => {
 //     matrix2.filter(t => t._1 == x._1).map(y=> y)
   // })

  }
}
