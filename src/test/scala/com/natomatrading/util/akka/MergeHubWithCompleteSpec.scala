/**
  * Copyright (C) 2015-2018 Lightbend Inc. <https://www.lightbend.com>
  */

package com.natomatrading.util.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.StreamTestKit._
import akka.testkit.EventFilter
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.language.postfixOps

class MergeHubWithCompleteSpec extends FlatSpec with Matchers with ScalaFutures {
  implicit val patience = PatienceConfig(timeout = 500 millis)
  implicit val timeout = Timeout(patienceConfig.timeout)

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  behavior of "MergeHubWithComplete"

  it should "work in the happy case" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(20).toMat(Sink.seq)(Keep.both).run()
    Source(1 to 10).runWith(sink)
    Source(11 to 20).runWith(sink)

    result.futureValue.sorted should ===(1 to 20)
  }

   it should "notify new producers if consumer cancels before first producer" in assertAllStagesStopped {
     val sink = Sink.cancelled[Int].runWith(MergeHubWithComplete.source[Int](16))
     val upstream = TestPublisher.probe[Int]()

     Source.fromPublisher(upstream).runWith(sink)

     upstream.expectCancellation()
   }

  it should "notify existing producers if consumer cancels after a few elements" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(5).toMat(Sink.seq)(Keep.both).run()
    val upstream = TestPublisher.probe[Int]()

    Source.fromPublisher(upstream).runWith(sink)
    for (i ← 1 to 5) upstream.sendNext(i)

    upstream.expectCancellation()
    result.futureValue.sorted should ===(1 to 5)
  }

  it should "notify new producers if consumer cancels after a few elements" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(5).toMat(Sink.seq)(Keep.both).run()
    val upstream1 = TestPublisher.probe[Int]()
    val upstream2 = TestPublisher.probe[Int]()

    Source.fromPublisher(upstream1).runWith(sink)
    for (i ← 1 to 5) upstream1.sendNext(i)

    upstream1.expectCancellation()
    result.futureValue.sorted should ===(1 to 5)

    Source.fromPublisher(upstream2).runWith(sink)

    upstream2.expectCancellation()
  }

  it should "work with long streams" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(20000).toMat(Sink.seq)(Keep.both).run()
    Source(1 to 10000).runWith(sink)
    Source(10001 to 20000).runWith(sink)

    result.futureValue.sorted should ===(1 to 20000)
  }

  it should "work with long streams when buffer size is 1" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](1).take(20000).toMat(Sink.seq)(Keep.both).run()
    val maybe1 = Source.maybe[Int].concat(Source(2 to 10000)).toMat(sink)(Keep.left).run()
    val maybe2 = Source.maybe[Int].concat(Source(10002 to 20000)).toMat(sink)(Keep.left).run()

    // give sources time to connect before starting to ensure the first source doesn't start until after the second
    // is attached
    Thread.sleep(100)

    maybe1.success(Some(1))
    maybe2.success(Some(10001))

    result.futureValue.sorted should ===(1 to 20000)
  }

  it should "work with long streams when consumer is slower" in assertAllStagesStopped {
    val (sink, result) =
      MergeHubWithComplete.source[Int](16)
        .take(2000)
        .throttle(10, 1.millisecond, 200, ThrottleMode.shaping)
        .toMat(Sink.seq)(Keep.both)
        .run()

    Source(1 to 1000).runWith(sink)
    Source(1001 to 2000).runWith(sink)

    result.futureValue.sorted should ===(1 to 2000)
  }

  it should "work with long streams if one of the producers is slower" in assertAllStagesStopped {
    val (sink, result) =
      MergeHubWithComplete.source[Int](16)
        .take(2000)
        .toMat(Sink.seq)(Keep.both)
        .run()

    Source(1 to 1000).throttle(10, 1.millisecond, 100, ThrottleMode.shaping).runWith(sink)
    Source(1001 to 2000).runWith(sink)

    result.futureValue.sorted should ===(1 to 2000)
  }

  it should "keep working even if one of the producers fail (traceback expected)" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(10).toMat(Sink.seq)(Keep.both).run()
    EventFilter.error("Upstream producer failed with exception").intercept {
      val maybe1 = Source.maybe[Int].toMat(sink)(Keep.left).run()
      val maybe2 = Source.maybe[Int].concat(Source(2 to 10)).toMat(sink)(Keep.left).run()

      // allow sources to start
      Thread.sleep(100)
      maybe1.failure(TE("failing"))
      maybe2.success(Some(1))
    }

    result.futureValue.sorted should ===(1 to 10)

  }
}
