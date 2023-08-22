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


}
