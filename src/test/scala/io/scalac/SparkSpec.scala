package io.scalac

import org.scalatest._

class SparkSpec extends FlatSpec with Matchers {

  "Spark" should "extended readme" in {
    Main.run("extended readme").isFirst("jakub.czuchnowski@gmail.com") shouldBe true
  }
  it should "readme" in {
    Main.run("readme").isFirst("pjazdzewski1990@gmail.com") shouldBe true
  }
  it should "Preliminary version of geecon blogpost" in {
    val keywords = "Preliminary version of geecon blogpost".toLowerCase
    Main.run(keywords).isFirst("lukasz@kuczera.me") shouldBe true
  }
  it should "added link for AngularJS internals in depth" in {
    val keywords = "added link for AngularJS internals in depth".toLowerCase
    Main.run(keywords).isFirst("marek.tomas@getnetworker.com") shouldBe true
  }
  it should "fix" in {
    val keywords = "fix".toLowerCase
    Main.run(keywords).isFirst("pjazdzewski1990@gmail.com") shouldBe true
  }
  it should "fixes" in {
    val keywords = "fixes".toLowerCase
    val r = Main.run(keywords)
    r.isFirst("pjazdzewski1990@gmail.com") shouldBe true
    r.isFirst("jakub.czuchnowski@gmail.com") shouldBe true
  }
  it should "Galaxy Gear app tutorial blog post" in {
    val keywords = "Galaxy Gear app tutorial blog post".toLowerCase
    Main.run(keywords).isFirst("anadoba@windowslive.com") shouldBe true
  }
  it should "Google Analytics" in {
    val keywords = "Google Analytics".toLowerCase
    Main.run(keywords).isFirst("jakub.czuchnowski@gmail.com") shouldBe true
  }
  it should "style css styling" in {
    val keywords = "style css styling".toLowerCase
    Main.run(keywords).isFirst("jakub.czuchnowski@gmail.com") shouldBe true
  }
  it should "hubspot rss twitter" in {
    val keywords = "hubspot rss twitter".toLowerCase
    Main.run(keywords).isFirst("pjazdzewski1990@gmail.com") shouldBe true
  }
  it should "Flow handler definition and explanation" in {
    val keywords = "Flow handler definition and explanation".toLowerCase
    Main.run(keywords).isFirst("marioosh@5dots.pl") shouldBe true
  }
  it should "Tomek: first draft of his 2 gotchas examples" in {
    val keywords = "Tomek: first draft of his 2 gotchas examples".toLowerCase
    Main.run(keywords).isFirst("tperek@arazoo.com") shouldBe true
  }
  it should "description for event sourcing at global scaled added" in {
    val keywords = "description for event sourcing at global scaled added".toLowerCase
    Main.run(keywords).isFirst("pierwszy1@gmail.com") shouldBe true
  }
  it should "exclude files from build" in {
    val keywords = "exclude files from build".toLowerCase
    Main.run(keywords).isFirst("jzubielik@users.noreply.github.com") shouldBe true
  }

  implicit class ArrayPimp(arr: Array[(String, Double, Seq[String])]) {
    def isFirst(candidate: String): Boolean = {
      val bestScore = arr.head._2
      val bestGroup = arr.groupBy(_._2)(bestScore)
      bestGroup.exists(_._1.contains(candidate))
    }
  }
}