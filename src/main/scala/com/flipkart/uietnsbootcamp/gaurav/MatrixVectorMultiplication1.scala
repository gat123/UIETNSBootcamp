package com.flipkart.uietnsbootcamp.gaurav


import org.apache.spark._

import scala.io.Source
/**
  * Created by gaurav.malhotra on 29/12/16.
  */
object MatrixVectorMultiplication1 {
  def main(args: Array[String]): Unit= {

    val conf = new SparkConf().setAppName("MyApp").setMaster("local")
    val sc = new SparkContext(conf)

    val matrix = sc.textFile("src/main/resources/MatrixSample.txt").map(line => line).
            zipWithIndex.flatMap(a => a._1.split(",").toList.zipWithIndex.map(b => (a._2.toLong, b._2.toLong, b._1.toDouble)))

    val vector = Source.fromFile("src/main/resources/VectorSample.txt").getLines.toList.
      map(line => (line.split(",")(0).toLong, line.split(",")(1).toDouble)).toMap

    val globalVector = sc.broadcast(vector)

    val matrixMult = matrix.map(value => (value._1, value._3*globalVector.value.get(value._2).getOrElse(0.0)))
                      .reduceByKey(_+_).collect()

    println(matrixMult.toList)

  }

}
