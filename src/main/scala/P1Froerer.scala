import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import au.com.bytecode.opencsv.CSVReader
import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level



object test10{





  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    // create a spark session
    System.setProperty("hadoop.home.dir", "C:\\Revature\\hadoop")
    System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse/")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    val out = new BufferedWriter(new FileWriter("C:/Revature/Output/Users.csv"))
    val writer = new CSVWriter(out)
    val UserSchema=Array("Username","Password")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")




    spark.sql("drop table if exists users")
    spark.sql("drop table if exists dogs")
    spark.sql("drop table if exists dogstmp")
    spark.sql("create table dogs(ID INT,name STRING,age DOUBLE,breed STRING,date_found DATE,adoptable_from DATE,posted DATE,color STRING,neutered STRING,housebroken STRING,likes_people STRING,likes_children STRING,get_along_males STRING,get_along_females STRING,get_along_cats STRING,keep_in STRING) PARTITIONED BY (sex STRING, size String, coat String) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("create table dogsTmp(ID INT,name STRING,age DOUBLE,sex STRING,breed STRING,date_found DATE,adoptable_from DATE,posted DATE,color STRING,coat STRING,size STRING,neutered STRING,housebroken STRING,likes_people STRING,likes_children STRING,get_along_males STRING,get_along_females STRING,get_along_cats STRING,keep_in STRING) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'ShelterDogs.csv' overwrite into table dogsTmp")
    spark.sql("insert overwrite table dogs partition(sex,size,coat) select ID,name,age,breed,date_found,adoptable_from,posted,color,neutered,housebroken,likes_people,likes_children,get_along_males,get_along_females,get_along_cats,keep_in,sex,size,coat from dogsTmp")
    val adminPass = "PJFP1PW"
    def menu(): Unit = {
      println("What would you like to do \n[1]Login as user \n[2]Login as admin \n[3]Create Account \n[4]exit")
        val x: Int = scala.io.StdIn.readInt()
        x match {
          case 1 => userLogin()
          case 2 => adminLogin()
          case 3 => createUser()
          case 4 => System.exit(0)
        }

    }

    def userLogin(): Unit = {
      println("Please enter your username:")
      val userName=scala.io.StdIn.readLine()
      println("And your password:")
      val userPass=scala.io.StdIn.readLine()
      searchMenu()
    }

    def adminLogin(): Unit = {
      println("please enter the Admin password")
      val adminPW = scala.io.StdIn.readLine()
        if (adminPW.equals(adminPass)){
          adminMenu()
        }
        else println("wrong password")
          adminLogin()
    }

    def createUser(): Unit = {
      println("Please enter a username")
      val newUser=scala.io.StdIn.readLine()
      println("Please enter a password")
      val newPass=scala.io.StdIn.readLine()
      val data=Array(s"$newUser",s"$newPass")
      writer.writeNext(data)
      out.close()
      println("you will now return to the menu to login")
      menu()
    }

    def searchMenu(): Unit = {
      println("What search would you like to run?" +
      "\n[1]What are the top 10 most common dog breeds given up for adoption?" +
      "\n[2]Are more male or female dogs given up for adoption, and which is older on average?" +
      "\n[3]My kid wants a small dog that's friendly, housetrained, younger than 3, and has short hair." +
      "\n[4]I'd like to get another rottweiler that has brown coloring." +
      "\n[5]Has the amount of dogs getting sheltered over the past 2 decades increased?" +
      "\n[6]Need a very friendly dog that is already housetrained and spayed/neutered, gets along with cats, and is fine staying inside." +
      "\n[7]Return to menu")
      val x: Int = scala.io.StdIn.readInt()
      x match {
        case 1 => spark.sql("select breed, count(breed) as breedcount from dogs WHERE NOT breed='Unknown Mix' group by breed order by breedcount DESC LIMIT 10").show(10)
          searchMenu()
        case 2 => spark.sql("select COUNT(ID) as Adoptable_Males_VS_Females, ROUND(AVG(age),2) as Avg_Female_and_Male_Age from dogs WHERE sex='male' UNION select COUNT(ID), ROUND(AVG(age),2) from dogs where sex='female'").show()
          searchMenu()
        case 3 => spark.sql("select name,age,color,breed from dogs where size = 'small' AND coat = 'short' AND housebroken = 'yes' AND likes_children = 'yes' AND age < 3").show()
          searchMenu()
        case 4 => spark.sql("select name,age,breed,color from dogs where lower(breed) like \"%rottweiler%\" AND lower(color) like \"%brown%\"").show()
          searchMenu()
        case 5 => spark.sql("select count(ID) as sheltered_dogs_per_5_years from dogs where date_found between '2001-01-01' and '2005-12-31' UNION select count(ID) from dogs where date_found between '2006-01-01' and '2010-12-31' UNION select count(ID) from dogs where date_found between '2011-01-01' and '2015-12-31' UNION select count(ID) from dogs WHERE date_found BETWEEN '2016-01-01' AND '2020-12-31' ORDER BY sheltered_dogs_per_5_years ASC").show()
          searchMenu()
        case 6 => spark.sql("SELECT name,breed,age,size,coat FROM dogs WHERE neutered='yes' AND housebroken = 'yes' AND likes_people = 'yes' AND likes_children = 'yes' and get_along_cats = 'yes' AND keep_in = 'flat'").show()
          searchMenu()
        case 7 => menu()
      }
    }
    def adminMenu(): Unit = {
      println("What would you like to do? \n[1]Go to the search menu \n[2]Edit table \n[3]Logout \n[4]Exit")
      val x: Int = scala.io.StdIn.readInt()
      x match {
        case 1 =>searchMenu()
        case 2 =>adminEdit()
        case 3 =>menu()
        case 4 =>System.exit(0)
      }
    }
    def adminEdit(): Unit = {
      println("What would you like to do? \n[1]Insert a row \n[2]Go back")
      val x: Int = scala.io.StdIn.readInt()
      x match {
        case 1 => adminInsert()
        case 2 => adminMenu()
      }
    }
    def adminInsert(): Unit = {
      spark.sql("select max(ID) as maxID from dogs").show()
      println("please enter as much info as you know about the dog (ID = one higher than current max)")
      val ID = scala.io.StdIn.readLine("ID:")
      val name = scala.io.StdIn.readLine("Name:")
      val age = scala.io.StdIn.readLine("age:")
      val breed = scala.io.StdIn.readLine("breed:")
      val dateFound = scala.io.StdIn.readLine("Date found (yyyy-mm-dd):")
      val adoptable = scala.io.StdIn.readLine("Date they're adoptable from (yyyy-mm-dd):")
      val posted = scala.io.StdIn.readLine("Date the adoption listing was posted (yyyy-mm-dd):")
      val color = scala.io.StdIn.readLine("Color:")
      val neut = scala.io.StdIn.readLine("Neutered or not (yes or no):")
      val housebroke = scala.io.StdIn.readLine("Housebroken (yes or no):")
      val peep = scala.io.StdIn.readLine("Likes people (yes or no):")
      val kids = scala.io.StdIn.readLine("Likes kids (yes or no):")
      val guys = scala.io.StdIn.readLine("Likes males (yes or no):")
      val girls = scala.io.StdIn.readLine("Likes females (yes or no):")
      val cats = scala.io.StdIn.readLine("Likes cats (yes or no):")
      val inOut = scala.io.StdIn.readLine("Where to keep them? (Flat, garden, or flat and garden):")
      val sex = scala.io.StdIn.readLine("Sex:")
      val size = scala.io.StdIn.readLine("size:")
      val coat = scala.io.StdIn.readLine("Coat/hair type:")
      spark.sql(s"insert into table dogs VALUES ($ID,'$name',$age,'$breed','$dateFound','$adoptable','$posted','$color','$neut','$housebroke','$peep','$kids','$guys','$girls','$cats','$inOut','$sex','$size','$coat')")
      println("insert complete, returning to admin menu")
      adminMenu()
    }

    menu()
  }
}







//possible queries/analyses
//I'd like to get another rottweiler that has brown coloring. select name,age,breed,color from dogs where lower(breed) like "%rottweiler%" AND lower(color) like "%brown%";
//What are the top 50 most common dog breeds given up for adoption? select breed, count(breed) as breedcount from dogs group by breed order by breedcount DESC LIMIT 50;
//Are more male or female dogs given up for adoption, and which is older on average? select COUNT(ID) as Adoptable_Males_VS_Females, ROUND(AVG(age),2) as Avg_Female_and_Male_Age from dogs WHERE sex='male' UNION select COUNT(ID), ROUND(AVG(age),2) from dogs where sex='female';
//Need a very friendly dog that is already housetrained and spayed/neutered, gets along with cats, and is fine staying inside. SELECT name,breed,age,size,coat FROM dogs WHERE neutered='yes' AND housebroken = 'yes' AND likes_people = 'yes' AND likes_children = 'yes' and get_along_cats = 'yes' AND keep _in = 'flat';
//My kid wants a small dog that's friendly, housetrained, younger than 3, and has short hair. select name,age,color,breed from dogs where size = 'small' AND coat = 'short' AND housebroken = 'yes' AND likes_children = 'yes' AND age < 3;
//Has the amount of dogs getting sheltered over the past 2 decades increased? select count(ID) as sheltered_dogs_per_5_years from dogs where date_found between '2001-01-01' and '2005-12-31' UNION select count(ID) from dogs where date_found between '2006-01-01' and '2010-12-31' UNION select count(ID) from dogs where date_found between '2011-01-01' and '2015-12-31' UNION select count(ID) from dogs where date_found between '2016-01-01' and '2020-12-31';

/*Starter stuff
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("drop table if exists dogs")
    spark.sql("drop table if exists dogstmp")
    spark.sql("create table dogs(ID INT,name STRING,age DOUBLE,breed STRING,date_found DATE,adoptable_from DATE,posted DATE,color STRING,neutered STRING,housebroken STRING,likes_people STRING,likes_children STRING,get_along_males STRING,get_along_females STRING,get_along_cats STRING,keep_in STRING) PARTITIONED BY (sex STRING, size String, coat String)  CLUSTERED BY (ID) INTO 3 buckets row format delimited fields terminated by ',' stored as textfile")
    spark.sql("create table dogsTmp(ID INT,name STRING,age DOUBLE,sex STRING,breed STRING,date_found DATE,adoptable_from DATE,posted DATE,color STRING,coat STRING,size STRING,neutered STRING,housebroken STRING,likes_people STRING,likes_children STRING,get_along_males STRING,get_along_females STRING,get_along_cats STRING,keep_in STRING) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'ShelterDogs.csv' overwrite into table dogsTmp")
    spark.sql("insert overwrite table dogs partition(sex,size,coat) select ID,name,age,breed,date_found,adoptable_from,posted,color,neutered,housebroken,likes_people,likes_children,get_along_males,get_along_females,get_along_cats,keep_in,sex,size,coat from dogsTmp")
    spark.sql("SELECT max(age) FROM dogs").show()
    spark.sql("select COUNT(ID) as Adoptable_Males_VS_Females, AVG(ROUND(age,2)) as Avg_Female_and_Male_Age from dogs WHERE sex='male' UNION select COUNT(ID), AVG(ROUND(age,2)) from dogs where sex='female'").show()

 */



//    val df2 = spark.read.csv("hdfs://localhost:9000/user/hive/warehouse/Bev_ConscountA.txt").collect().length
//    val Bev_ConscountA=spark.read.csv("hdfs://localhost:9000/user/hive/warehouse/Bev_ConscountA.txt").toDF()
//    Bev_ConscountA.createOrReplaceTempView("ConsA")
//    spark.sql("SELECT * FROM ConsA").show()