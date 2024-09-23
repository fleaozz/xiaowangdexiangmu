import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object data {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession，应用程序名称为 "Travel_Analyse"，并使用所有本地可用线程
    val spark = SparkSession.builder()
      .appName("Travel_Analyse")
      .master("local[*]") // 使用所有可用的本地线程
      .getOrCreate() // 如果存在，则获取 SparkSession，否则创建一个新的


    // 设置HDFS路径和MySQL数据库连接信息
    val basePath = "hdfs://192.168.2.99:9000/"
    val fileNamePattern = "data%s.csv"
    val url = "jdbc:mysql://192.168.2.99:3306/data" // 存入为data数据库的表
    val user = "root" // MySQL数据库用户名
    val password = "root" // MySQL数据库密码


    // 定义要处理的文件编号并处理
    val fileNumbers = Seq(518, 520, 525, 528, 530, 531)
    fileNumbers.foreach { i =>
      val filePath = basePath + fileNamePattern.format(i)
      val tableName = s"travel_$i"

      // 读取CSV文件到DataFrame
      val df = spark.read.option("header", true).csv(filePath)

      // 选择和分组操作
      val districtDF = df.select("travel_city", "travel_name", "travel_total", "travel_place", "travel_notes_url")
        .na.drop("any")
        .groupBy("travel_place")
        .count()

      // 合并数据
      val resultDF = df.join(districtDF, Seq("travel_place"), "inner")

      // 写入到MySQL数据库
      resultDF.write
        .mode(SaveMode.Overwrite) // 如果表已经存在，覆盖写入
        .jdbc(url, tableName, new java.util.Properties() { //Properties()用于传递数据库连接用户名和密码
          {
            put("user", user) // MySQL数据库的用户名
            put("password", password) // MySQL数据库的密码
          }
        })
    }



    // 循环处理从601到630的CSV文件 6月的csv
    (601 to 630).foreach { i =>
      if (i != 617) {
        val filePath = basePath + fileNamePattern.format(i)
        val tableName = s"travel_$i"

        // 读取CSV文件
        val df = spark.read.option("header", true).csv(filePath)

        // 选择和分组操作
        val districtDF = df.select("travel_city", "travel_name", "travel_total", "travel_place", "travel_notes_url")
          .na.drop("any")
          .groupBy("travel_place")
          .count()

        // 合并数据
        val resultDF = df.join(districtDF, Seq("travel_place"), "inner")

        // 写入到MySQL数据库
        resultDF.write
          .mode(SaveMode.Overwrite) // 如果表已经存在，覆盖写入
          .jdbc(url, tableName, new java.util.Properties() {
            {
              put("user", user) // MySQL数据库的用户名
              put("password", password) // MySQL数据库的密码
            }
          })
      }
    }
    // 循环处理从701到731的CSV文件 7月的csv

    (701 to 731).foreach { i =>
      if (i < 716 || i > 724) {
        val filePath = basePath + fileNamePattern.format(i)
        val tableName = s"travel_$i"

        // 读取CSV文件
        val df = spark.read.option("header", true).csv(filePath)

        // 选择和分组操作
        val districtDF = df.select("travel_city", "travel_name", "travel_total", "travel_place", "travel_notes_url")
          .na.drop("any")
          .groupBy("travel_place")
          .count()

        // 合并数据
        val resultDF = df.join(districtDF, Seq("travel_place"), "inner")

        // 写入到MySQL数据库
        resultDF.write
          .mode(SaveMode.Overwrite) // 如果表已经存在，覆盖写入
          .jdbc(url, tableName, new java.util.Properties() {
            {
              put("user", user) // MySQL数据库的用户名
              put("password", password) // MySQL数据库的密码
            }
          })
      }
    }



    // 循环处理从801到826的CSV文件 8月的csv
    (801 to 830).foreach { i =>
      val filePath = basePath + fileNamePattern.format(i)
      val tableName = s"travel_$i"

      // 读取CSV文件
      val df = spark.read.option("header", true).csv(filePath)

      // 选择和分组操作
      val districtDF = df.select("travel_city", "travel_name", "travel_total", "travel_place", "travel_notes_url")
        .na.drop("any")
        .groupBy("travel_place")
        .count()

      // 合并数据
      val resultDF = df.join(districtDF, Seq("travel_place"), "inner")

      // 写入到MySQL数据库
      resultDF.write
        .mode(SaveMode.Overwrite) // 如果表已经存在，覆盖写入
        .jdbc(url, tableName, new java.util.Properties() {
          {
            put("user", user) // MySQL数据库的用户名
            put("password", password) // MySQL数据库的密码
          }
        })
    }

    // 循环处理从701到731的CSV文件 7月的csv

    (901 to 915).foreach { i =>
      if (i <= 907 || i >= 910 ) {
        val filePath = basePath + fileNamePattern.format(i)
        val tableName = s"travel_$i"

        // 读取CSV文件
        val df = spark.read.option("header", true).csv(filePath)

        // 选择和分组操作
        val districtDF = df.select("travel_city", "travel_name", "travel_total", "travel_place", "travel_notes_url")
          .na.drop("any")
          .groupBy("travel_place")
          .count()

        // 合并数据
        val resultDF = df.join(districtDF, Seq("travel_place"), "inner")

        // 写入到MySQL数据库
        resultDF.write
          .mode(SaveMode.Overwrite) // 如果表已经存在，覆盖写入
          .jdbc(url, tableName, new java.util.Properties() {
            {
              put("user", user) // MySQL数据库的用户名
              put("password", password) // MySQL数据库的密码
            }
          })
      }
    }




    val df16 = spark.read.option("header", true).csv("hdfs://192.168.2.99:9000/data16.csv")

    //景区前八、美食前八
    val districtDF16 = df16.select("travel_city", "travel_cite", "travel_cite_mark", "travel_food", "travel_food_mark")
      .groupBy("travel_city")
      .count()
    val resultDF16 = df16.join(districtDF16, Seq("travel_city"), "inner")
    resultDF16.write
      .mode(SaveMode.Overwrite) // 如果表已经存在，覆盖写入
      .jdbc(url, "travel_16", new java.util.Properties() {
        {
          put("user", user) // 虚拟机MySQL数据库的用户名
          put("password", password) // 虚拟机MySQL数据库的密码
        }
      })

    // 停止 SparkSession
    spark.stop()
  }
}