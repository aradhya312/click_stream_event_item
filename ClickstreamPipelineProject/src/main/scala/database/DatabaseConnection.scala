package database
//import database.DatabaseConnection.spark
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.DriverManager
import org.apache.spark.sql.SparkSession
import service.FileReader
import utils.sparksession
import java.sql.{Connection, DriverManager}

    object DatabaseConnection {
        def getConnection(): Connection = {
            val properties = new java.util.Properties()
            properties.setProperty("user", constant.jdbcUser)
            properties.setProperty("password", constant.jdbcPassword)
            DriverManager.getConnection(constant.jdbcUrl, properties)

        }

        //        private val url = "jdbc:mysql://localhost:3306/clickstream_data_events"
//        private val properties = new java.util.Properties()
//        properties.setProperty("user", "root")
//        properties.setProperty("password", "Aradhya@123")
//        def getConnection(): Connection = {
//            DriverManager.getConnection(url, properties)
//        }
}
