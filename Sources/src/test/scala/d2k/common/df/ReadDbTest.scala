package d2k.common.df

import org.scalatest.WordSpec
import org.scalatest.MustMatchers
import org.scalatest.BeforeAndAfter
import d2k.common.TestArgs
import d2k.common.InputArgs
import d2k.common.df.template.DbToDf
import d2k.common.df.executor.Nothing
import spark.common.DbCtl
import org.apache.spark.sql.SaveMode

class ReadDbTest extends WordSpec with MustMatchers with BeforeAndAfter {
  implicit val inArgs = TestArgs().toInputArgs
  "SingleReadDb" should {
    "defined columns" in {
      val readDb = new SingleReadDb {
        val componentId = "compo"
        override lazy val readTableName = "SP02"
        override val columns = Array("DT", "VC", "CH")
      }
      val result = readDb.readDb(inArgs).collect
      result.size mustBe 4
      result.head.schema.size mustBe 3
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe false
      names.exists(_ == "NUM52") mustBe false
      names.exists(_ == "TSTMP") mustBe false
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true
    }

    "not defined columns" in {
      val readDb = new SingleReadDb {
        val componentId = "compo"
        override lazy val readTableName = "SP02"
      }
      val result = readDb.readDb.collect
      result.size mustBe 4
      result.head.schema.size mustBe 6
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe true
      names.exists(_ == "NUM52") mustBe true
      names.exists(_ == "TSTMP") mustBe true
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true
    }

    "defined column and readDbWhere " in {
      val readDb = new SingleReadDb {
        val componentId = "compo"
        override lazy val readTableName = "SP02"
        override val columns = Array("DT", "NUM5", "VC", "CH")
        override val readDbWhere = Array("NUM5 = '2000'")
      }
      val result = readDb.readDb.collect
      result.size mustBe 1
      result.head.schema.size mustBe 4
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe true
      names.exists(_ == "NUM52") mustBe false
      names.exists(_ == "TSTMP") mustBe false
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true

      result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
    }

    "defined column and readDbWhereArgs " in {
      val readDb = new SingleReadDb {
        val componentId = "compo"
        override lazy val readTableName = "SP02"
        override val columns = Array("DT", "NUM5", "VC", "CH")
        override def readDbWhere(inArgs: InputArgs) = Array("NUM5 = '2000'")
      }
      val result = readDb.readDb.collect
      result.size mustBe 1
      result.head.schema.size mustBe 4
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe true
      names.exists(_ == "NUM52") mustBe false
      names.exists(_ == "TSTMP") mustBe false
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true

      result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
    }

    "not defined column and defined readDbWhere " in {
      val readDb = new SingleReadDb {
        val componentId = "compo"
        override lazy val readTableName = "SP02"
        override val readDbWhere = Array("NUM5 = '2000'")
      }
      val result = readDb.readDb.collect
      result.size mustBe 1
      result.head.schema.size mustBe 6
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe true
      names.exists(_ == "NUM52") mustBe true
      names.exists(_ == "TSTMP") mustBe true
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true

      result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
    }

    "not defined column and defined readDbWhereArgs " in {
      val readDb = new SingleReadDb {
        val componentId = "compo"
        override lazy val readTableName = "SP02"
        override def readDbWhere(inArgs: InputArgs) = Array("NUM5 = '2000'")
      }
      val result = readDb.readDb.collect
      result.size mustBe 1
      result.head.schema.size mustBe 6
      val names = result.head.schema.map(_.name)
      names.exists(_ == "DT") mustBe true
      names.exists(_ == "NUM5") mustBe true
      names.exists(_ == "NUM52") mustBe true
      names.exists(_ == "TSTMP") mustBe true
      names.exists(_ == "VC") mustBe true
      names.exists(_ == "CH") mustBe true

      result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
    }
  }

  "MultiReadDb" should {
    val copyDb = new DbToDf with Nothing {
      val componentId = "compo"
      override lazy val readTableName = "SP02"
    }

    val dbCtl = new DbCtl(copyDb.readDbInfo)
    import dbCtl.implicits._
    copyDb.run(Unit).write.mode(SaveMode.Overwrite).jdbc(dbCtl.dbInfo.url, "SP02_tmp", dbCtl.props)

    "defined columns" in {
      val readDb = new MultiReadDb {
        val componentId = "compo"
        val readTableNames = Seq("SP02", "SP02_tmp")
        override val columns = Array("DT", "VC", "CH")
      }
      readDb.readDb(inArgs).foreach { data =>
        val result = data._2.collect
        result.size mustBe 4
        result.head.schema.size mustBe 3
        val names = result.head.schema.map(_.name)
        names.exists(_ == "DT") mustBe true
        names.exists(_ == "NUM5") mustBe false
        names.exists(_ == "NUM52") mustBe false
        names.exists(_ == "TSTMP") mustBe false
        names.exists(_ == "VC") mustBe true
        names.exists(_ == "CH") mustBe true
      }
    }

    "not defined columns" in {
      val readDb = new MultiReadDb {
        val componentId = "compo"
        val readTableNames = Seq("SP02", "SP02_tmp")
      }
      readDb.readDb(inArgs).foreach { data =>
        val result = data._2.collect
        result.size mustBe 4
        result.head.schema.size mustBe 6
        val names = result.head.schema.map(_.name)
        names.exists(_ == "DT") mustBe true
        names.exists(_ == "NUM5") mustBe true
        names.exists(_ == "NUM52") mustBe true
        names.exists(_ == "TSTMP") mustBe true
        names.exists(_ == "VC") mustBe true
        names.exists(_ == "CH") mustBe true
      }
    }

    "defined column and readDbWhere " in {
      val readDb = new MultiReadDb {
        val componentId = "compo"
        val readTableNames = Seq("SP02", "SP02_tmp")
        override val columns = Array("DT", "NUM5", "VC", "CH")
        override val readDbWhere = Array("NUM5 = '2000'")
      }
      readDb.readDb(inArgs).foreach { data =>
        val result = data._2.collect
        result.size mustBe 1
        result.head.schema.size mustBe 4
        val names = result.head.schema.map(_.name)
        names.exists(_ == "DT") mustBe true
        names.exists(_ == "NUM5") mustBe true
        names.exists(_ == "NUM52") mustBe false
        names.exists(_ == "TSTMP") mustBe false
        names.exists(_ == "VC") mustBe true
        names.exists(_ == "CH") mustBe true
        result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
      }
    }

    "defined column and readDbWhereArgs " in {
      val readDb = new MultiReadDb {
        val componentId = "compo"
        val readTableNames = Seq("SP02", "SP02_tmp")
        override val columns = Array("DT", "NUM5", "VC", "CH")
        override def readDbWhere(inArgs: InputArgs) = Array("NUM5 = '2000'")
      }
      readDb.readDb(inArgs).foreach { data =>
        val result = data._2.collect
        result.size mustBe 1
        result.head.schema.size mustBe 4
        val names = result.head.schema.map(_.name)
        names.exists(_ == "DT") mustBe true
        names.exists(_ == "NUM5") mustBe true
        names.exists(_ == "NUM52") mustBe false
        names.exists(_ == "TSTMP") mustBe false
        names.exists(_ == "VC") mustBe true
        names.exists(_ == "CH") mustBe true

        result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
      }
    }

    "not defined column and defined readDbWhere " in {
      val readDb = new MultiReadDb {
        val componentId = "compo"
        val readTableNames = Seq("SP02", "SP02_tmp")
        override val readDbWhere = Array("NUM5 = '2000'")
      }
      readDb.readDb(inArgs).foreach { data =>
        val result = data._2.collect
        result.size mustBe 1
        result.head.schema.size mustBe 6
        val names = result.head.schema.map(_.name)
        names.exists(_ == "DT") mustBe true
        names.exists(_ == "NUM5") mustBe true
        names.exists(_ == "NUM52") mustBe true
        names.exists(_ == "TSTMP") mustBe true
        names.exists(_ == "VC") mustBe true
        names.exists(_ == "CH") mustBe true

        result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
      }
    }

    "not defined column and defined readDbWhereArgs " in {
      val readDb = new MultiReadDb {
        val componentId = "compo"
        val readTableNames = Seq("SP02", "SP02_tmp")
        override def readDbWhere(inArgs: InputArgs) = Array("NUM5 = '2000'")
      }
      readDb.readDb(inArgs).foreach { data =>
        val result = data._2.collect
        result.size mustBe 1
        result.head.schema.size mustBe 6
        val names = result.head.schema.map(_.name)
        names.exists(_ == "DT") mustBe true
        names.exists(_ == "NUM5") mustBe true
        names.exists(_ == "NUM52") mustBe true
        names.exists(_ == "TSTMP") mustBe true
        names.exists(_ == "VC") mustBe true
        names.exists(_ == "CH") mustBe true

        result(0).getAs[java.math.BigDecimal]("NUM5").toString mustBe "2000"
      }
    }
  }
}