/**
  * Copyright (C) 2015-2018 Lightbend Inc. <https://www.lightbend.com>
  */

package com.natomatrading.util.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.ThrottleMode
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.StreamTestKit._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Success

class MergeHubWithCompleteSpec extends AsyncFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var system:ActorSystem = null
  implicit var ec:ExecutionContext = null

  override def beforeAll(): Unit = {
    system = ActorSystem()
    ec = system.dispatcher
  }

  override def afterAll(): Unit = {
    Thread.sleep(1000)
    system.terminate()
    system = null
    ec = null
  }

  behavior of "MergeHubWithComplete"

  it should "work in the happy case" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(20).toMat(Sink.seq)(Keep.both).run()
    val promise1:Promise[Option[Int]] = Source.maybe[Int].concat(Source(1 to 10)).toMat(sink)(Keep.left).run()
    val promise2:Promise[Option[Int]] = Source.maybe[Int].concat(Source(11 to 20)).toMat(sink)(Keep.left).run()

    promise1.success(None)
    promise2.success(None)

    result map { _.sorted } transformWith { _ should ===(Success(1 to 20)) }
  }

   it should "notify new producers if consumer cancels before first producer" in assertAllStagesStopped {
     val sink = Sink.cancelled[Int].runWith(MergeHubWithComplete.source[Int](16))
     val upstream = TestPublisher.probe[Int]()

     Source.fromPublisher(upstream).runWith(sink)

     Future { upstream.expectCancellation(); succeed }
   }

  it should "notify existing producers if consumer cancels after a few elements" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(5).toMat(Sink.seq)(Keep.both).run()
    val upstream = TestPublisher.probe[Int]()

    Source.fromPublisher(upstream).runWith(sink)
    for (i <- 1 to 5) upstream.sendNext(i)

    upstream.expectCancellation()
    result map {_.sorted} transformWith {_ should ===(Success(1 to 5))}
  }

  it should "notify new producers if consumer cancels after a few elements" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(5).toMat(Sink.seq)(Keep.both).run()
    val upstream1 = TestPublisher.probe[Int]()
    val upstream2 = TestPublisher.probe[Int]()

    Source.fromPublisher(upstream1).runWith(sink)
    for (i <- 1 to 5) upstream1.sendNext(i)

    Future { upstream1.expectCancellation() } flatMap { _ =>
      result map {_.sorted} transformWith {_ should ===(Success(1 to 5))}
    } flatMap { _ =>
      Source.fromPublisher(upstream2).runWith(sink)
      Future {
        upstream2.expectCancellation()
        succeed
      }
    }
  }

  it should "work with long streams" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16).take(20000).toMat(Sink.seq)(Keep.both).run()
    Source(1 to 10000).runWith(sink)
    Source(10001 to 20000).runWith(sink)

    result map {_.sorted} transformWith {_ should ===(Success(1 to 20000))}
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

    result map {_.sorted} transformWith {_ should ===(Success(1 to 20000)) }
  }

  it should "work with long streams when consumer is slower" in assertAllStagesStopped {
    val (sink, result) = MergeHubWithComplete.source[Int](16)
        .take(2000)
        .throttle(10, 1.millisecond, 200, ThrottleMode.shaping)
        .toMat(Sink.seq)(Keep.both)
        .run()

    Source(1 to 1000).runWith(sink)
    Source(1001 to 2000).runWith(sink)

    result map {_.sorted} transformWith {_ should ===(Success(1 to 2000)) }
  }

  it should "work with long streams if one of the producers is slower" in assertAllStagesStopped {
    val (sink, result) =
      MergeHubWithComplete.source[Int](16)
        .take(2000)
        .toMat(Sink.seq)(Keep.both)
        .run()

    Source(1 to 1000).throttle(10, 1.millisecond, 100, ThrottleMode.shaping).runWith(sink)
    Source(1001 to 2000).runWith(sink)

    result map {_.sorted} transformWith {_ should ===(Success(1 to 2000)) }
  }

  it should "stop working when one of the producers fails" in assertAllStagesStopped {

    class ThisTestException extends Exception("private exception for this test")

    val (sink, result) = MergeHubWithComplete.source[Int](16).toMat(Sink.seq)(Keep.both).run()
    val maybe1 = Source.maybe[Int].toMat(sink)(Keep.left).run()
    val maybe2 = Source.maybe[Int].concat(Source(2 to 10)).toMat(sink)(Keep.left).run()

    recoverToSucceededIf[ThisTestException] {
      Future {
        blocking {
          Thread.sleep(100)
        }
      } flatMap { _ =>
        maybe1.failure(new ThisTestException)
        maybe2.success(Some(1))
        result
      }
    }
  }
}
