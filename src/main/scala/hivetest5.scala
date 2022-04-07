import org.apache.spark.sql.SparkSession

object hivetest5 {
  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("hadoop.home.dir", "C:\\revature\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE src")
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    spark.sql("create table IF NOT EXISTS newone(id Int,name String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE newone")
    spark.sql("SELECT * FROM newone").show()
//    spark.sql("DROP TABLE IF EXISTS Recipes")
//    spark.sql("create table IF NOT EXISTS Recipes(id INT, name STRING, description STRING, ingredients STRING, ingredients_raw_str STRING, serving_size STRING, servings INT, steps STRING, tags STRING, search_terms STRING) row format delimited fields terminated by ','");
//    spark.sql("LOAD DATA LOCAL INPATH 'Recipes.csv' INTO TABLE Recipes")
//    spark.sql("SELECT * FROM Recipes").show()
//
//    spark.sql("create table IF NOT EXISTS Recipes(id INT, name STRING, description STRING, ingredients_raw_str STRING, serving_size STRING, servings INT, steps STRING, tags STRING, search_terms STRING) PARTITIONED BY (ingredients STRING) row format delimited fields terminated by ','");


  }
}
