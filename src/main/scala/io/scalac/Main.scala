package io.scalac

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable


object Main extends App{
  println(s"Starting")

  val target = "description for event sourcing at global scaled added".toLowerCase

  val similarity = run(target)
  println(s"Who Said $target?")
  println(s"${similarity.mkString("\n")}")

  def run(target: String): Array[(String, Double, Seq[String])] = {
    val conf = new SparkConf().setAppName("Who said that").setMaster("local[2]")
    val spark = new SparkContext(conf)

    val rawData = cleanData(spark, "git_log.txt")
    //  rawData.saveAsTextFile("git_log_clean.txt")

    val authorAndCommits = groupData(rawData.collect())
    //  authorAndCommits.foreach{ case (a, cs) => println(s"$a said: \n  ${cs.mkString("\n  ")}")}
    val authorAndCommitsRDD = spark.parallelize(authorAndCommits.toSeq)

    val similarity = countSimilarity(target, authorAndCommitsRDD).collect()

    println(s"Ending")
    spark.stop()

    similarity
  }

  private def cleanData(spark: SparkContext, file: String) = {
    spark.textFile(file).filter { line =>
      line.contains("Author") || (line.startsWith(" ") && line.trim.length > 0)
    }.map(_.trim.toLowerCase.replaceAll("\"", "").replaceAll("\'", ""))
  }

  private def groupData(rawData: Array[String]) = {
    val authorAndCommits = mutable.Map[String, Seq[String]]()
    rawData.foldLeft(""){ case (currentAuthor, line) =>
      if(line.startsWith("author")){
        line.replace("author: ", "") /// this is the new author
      }else {
        authorAndCommits(currentAuthor) = authorAndCommits.getOrElse(currentAuthor, Seq.empty[String]) :+ line
        currentAuthor /// update the current author data and move on
      }
    }
    authorAndCommits
  }

  def countSimilarity(target: String, rdd: RDD[(String, Seq[String])]): RDD[(String, Double, Seq[String])] = {
    val mapped = rdd.map {
      case (author, commits) =>
        val words = commits.flatMap(_.split(" "))
        (author, words)
    }
    countSimilarity(target.split(" ").filter(_.length > 3), mapped)
  }

  private def countSimilarity(target: Array[String], rdd: RDD[(String, Seq[String])]): RDD[(String, Double, Seq[String])] = {
    rdd.map {
      case (author, words) =>
        val matching = (for {
          w <- words
          t <- target
          if t.length > 3 && w.length > 3 //remove short words
          score = scoreSimilarity(t, w)
          if score > 0.65d //remove accidental, short matches
        } yield (score, w)).distinct //remove duplicated results

        val score = Math.min(matching.map(_._1).sum, target.length)
        val wordsMatched = matching.map(_._2)

        (author, score, wordsMatched)
    }.sortBy(_._2, false)
  }

//  def scoreSimilarity(s1: String, s2: String): Double = if (s1 == s2){
//    1
//  } else 0

//  def scoreSimilarity(s1: String, s2: String): Double = {
//    val len = Math.min(s1.length, s2.length)
//    val sum: Double = (for {
//      i <- 0 until len
//      v = if(s1.charAt(i) == s2.charAt(i)) 1.0 else 0.0
//    } yield v).sum
//
//    sum/len
//  }

  def scoreSimilarity(s1: String, s2: String): Double = {
    val len = Math.min(s1.length, s2.length)
    val sum: Double = (for {
      i <- 0 until len
      v = if(s1.charAt(i) == s2.charAt(i)) 1.0 else 0.0
    } yield v).sum

    Math.pow(sum, 2.0)/Math.pow(len, 2.0)
  }
}
