package com.availity.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec

abstract class SparkBaseSpec extends  AnyFunSpec
with BeforeAndAfter
with BeforeAndAfterEach {
  implicit def sparkSession: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
}
