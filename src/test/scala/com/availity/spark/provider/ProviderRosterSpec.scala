package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import com.availity.spark.SparkBaseSpec
import org.apache.spark.sql.functions.col

class ProviderRosterSpec extends SparkBaseSpec with DataFrameComparer with BeforeAndAfterEach {

  override def beforeEach: Unit = {
  }

  override def afterEach(): Unit = {
  }

  describe("process") {

    it("should read the providers dataset") {
      val df = ProviderRoster.readProviderDs("data/providers.csv")
      assert( 1000 ==df.count())
      assert(df.columns.length == 6)
    }

    it("should read the visits dataset") {
      val df = ProviderRoster.readVisitorDs("data/visits.csv")
      assert( 22348 ==df.count())
      assert(df.columns.length == 3)
    }

    it("should get provider speciality visitor data") {
      val providerDf = ProviderRoster.readProviderDs("data/providers.csv")
      val visitorDf = ProviderRoster.readVisitorDs("data/visits.csv")
      val joinDf = ProviderRoster.getJoinDf(providerDf, visitorDf)
      val providerSpecialityDf = ProviderRoster.getVisitorsBySpecality(joinDf)
        .filter(col("provider_id") === "24060" &&
          col("name") === "WoodrowALabadie" &&
          col("provider_specialty") === "Psychiatry")
      assert(16 == providerSpecialityDf.select("number_of_visits").head().getLong(0))
    }

    it("should get provider visitor by date data") {

      val visitorDf = ProviderRoster.readVisitorDs("data/visits.csv")
      val providerSpecialityDf = ProviderRoster.getVisitorsByDate(visitorDf)
        .filter(col("provider_id") === "99399" &&
          col("service_date") === "2021-10-07")
      assert(2 == providerSpecialityDf.select("number_of_visits").head().getLong(0))
    }

    it("should run a test") {
      ProviderRoster.process()
    }


  }
}
