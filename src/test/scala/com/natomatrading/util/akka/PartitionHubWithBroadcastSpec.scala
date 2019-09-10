/**
  * Copyright (C) 2015-2018 Lightbend Inc. <https://www.lightbend.com>
  * Copyright (C) 2018 Joseph Malicki
  */

package com.natomatrading.util.akka

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, KillSwitches, ThrottleMode}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.Await
import org.scalatest.concurrent.ScalaFutures

import scala.language.postfixOps

class PartitionHubWithBroadcastSpec extends FlatSpec with Matchers with ScalaFutures {
  override implicit val patienceConfig = PatienceConfig(timeout = 300 millis)


  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  behavior of "PartitionHubWithBroadcast"

  it should "work in the happy case with one stream" in assertAllStagesStopped {
    val source = Source(1 to 10).runWith(PartitionHubWithBroadcast.sink((size, elem) ⇒ 0, startAfterNrOfConsumers = 0, bufferSize = 8))
    source.runWith(Sink.seq).futureValue should ===(1 to 10)
  }

  it should "work in the happy case with two streams" in assertAllStagesStopped {
    val source = Source(0 until 10).runWith(PartitionHubWithBroadcast.sink((size, elem) ⇒ elem % size, startAfterNrOfConsumers = 2, bufferSize = 8))
    val (future1, result1) = source.toMat(Sink.seq)(Keep.both).run()

    // Ensure that this future goes first
    Await.ready(future1, 20.millis)

    val (future2, result2) = source.toMat(Sink.seq)(Keep.both).run()

    result1.futureValue should ===(0 to 8 by 2)
    result2.futureValue should ===(1 to 9 by 2)
  }

  it should "be able to use as round-robin router" in assertAllStagesStopped {
    val source = Source(0 until 10).runWith(PartitionHubWithBroadcast.statefulSink(() ⇒ {
      var n = 0L

      (info, elem) ⇒ {
        n += 1
        info.consumerIdByIdx((n % info.size).toInt)
      }
    }, startAfterNrOfConsumers = 2, bufferSize = 8))
    val result1 = source.runWith(Sink.seq)
    val result2 = source.runWith(Sink.seq)
    result1.futureValue should ===(1 to 9 by 2)
    result2.futureValue should ===(0 to 8 by 2)
  }

  it should "be able to use as sticky session router" in assertAllStagesStopped {
    val source = Source(List("usr-1", "usr-2", "usr-1", "usr-3")).runWith(PartitionHubWithBroadcast.statefulSink(() ⇒ {
      var sessions = Map.empty[String, Long]
      var n = 0L

      (info, elem) ⇒ {
        sessions.get(elem) match {
          case Some(id) if info.consumerIds.exists(_ == id) ⇒ id
          case _ ⇒
            n += 1
            val id = info.consumerIdByIdx((n % info.size).toInt)
            sessions = sessions.updated(elem, id)
            id
        }
      }
    }, startAfterNrOfConsumers = 2, bufferSize = 8))
    val result1 = source.runWith(Sink.seq)
    val result2 = source.runWith(Sink.seq)
    result1.futureValue should ===(List("usr-2"))
    result2.futureValue should ===(List("usr-1", "usr-1", "usr-3"))
  }

  it should "be able to use as fastest consumer router" in assertAllStagesStopped {
    val source = Source(0 until 1000).runWith(PartitionHubWithBroadcast.statefulSink(
      () ⇒ (info, elem) ⇒ info.consumerIds.toVector.minBy(id ⇒ info.queueSize(id)),
      startAfterNrOfConsumers = 2, bufferSize = 4))
    val result1 = source.runWith(Sink.seq)
    val result2 = source.throttle(10, 100.millis, 10, ThrottleMode.Shaping).runWith(Sink.seq)

    result1.futureValue.size should be > (result2.futureValue.size)
  }

  it should "route evenly" in assertAllStagesStopped {
    val (testSource, hub) = TestSource.probe[Int].toMat(
      PartitionHubWithBroadcast.sink((size, elem) ⇒ elem % size, startAfterNrOfConsumers = 2, bufferSize = 8))(Keep.both).run()
    val probe0 = hub.runWith(TestSink.probe[Int])
    val probe1 = hub.runWith(TestSink.probe[Int])
    probe0.request(3)
    probe1.request(10)
    testSource.sendNext(0)
    probe0.expectNext(0)
    testSource.sendNext(1)
    probe1.expectNext(1)

    testSource.sendNext(2)
    testSource.sendNext(3)
    testSource.sendNext(4)
    probe0.expectNext(2)
    probe1.expectNext(3)
    probe0.expectNext(4)

    // probe1 has not requested more
    testSource.sendNext(5)
    testSource.sendNext(6)
    testSource.sendNext(7)
    probe1.expectNext(5)
    probe1.expectNext(7)
    probe0.expectNoMessage(10.millis)
    probe0.request(10)
    probe0.expectNext(6)

    testSource.sendComplete()
    probe0.expectComplete()
    probe1.expectComplete()
  }

  it should "route unevenly" in assertAllStagesStopped {
    val (testSource, hub) = TestSource.probe[Int].toMat(
      PartitionHubWithBroadcast.sink((size, elem) ⇒ (elem % 3) % 2, startAfterNrOfConsumers = 2, bufferSize = 8))(Keep.both).run()
    val probe0 = hub.runWith(TestSink.probe[Int])
    val probe1 = hub.runWith(TestSink.probe[Int])

    // (_ % 3) % 2
    // 0 => 0
    // 1 => 1
    // 2 => 0
    // 3 => 0
    // 4 => 1

    probe0.request(10)
    probe1.request(10)
    testSource.sendNext(0)
    probe0.expectNext(0)
    testSource.sendNext(1)
    probe1.expectNext(1)
    testSource.sendNext(2)
    probe0.expectNext(2)
    testSource.sendNext(3)
    probe0.expectNext(3)
    testSource.sendNext(4)
    probe1.expectNext(4)

    testSource.sendComplete()
    probe0.expectComplete()
    probe1.expectComplete()
  }

  it should "backpressure" in assertAllStagesStopped {
    val (testSource, hub) = TestSource.probe[Int].toMat(
      PartitionHubWithBroadcast.sink((size, elem) ⇒ 0, startAfterNrOfConsumers = 2, bufferSize = 4))(Keep.both).run()
    val probe0 = hub.runWith(TestSink.probe[Int])
    val probe1 = hub.runWith(TestSink.probe[Int])
    probe0.request(10)
    probe1.request(10)
    testSource.sendNext(0)
    probe0.expectNext(0)
    testSource.sendNext(1)
    probe0.expectNext(1)
    testSource.sendNext(2)
    probe0.expectNext(2)
    testSource.sendNext(3)
    probe0.expectNext(3)
    testSource.sendNext(4)
    probe0.expectNext(4)

    testSource.sendComplete()
    probe0.expectComplete()
    probe1.expectComplete()
  }

  it should "ensure that from two different speed consumers the slower controls the rate" in assertAllStagesStopped {
    val (firstElem, source) = Source.maybe[Int].concat(Source(1 until 20)).toMat(
      PartitionHubWithBroadcast.sink((size, elem) ⇒ elem % size, startAfterNrOfConsumers = 2, bufferSize = 1))(Keep.both).run()

    val f1 = source.throttle(1, 10.millis, 1, ThrottleMode.shaping).runWith(Sink.seq)
    // Second cannot be overwhelmed since the first one throttles the overall rate, and second allows a higher rate
    val f2 = source.throttle(10, 10.millis, 8, ThrottleMode.enforcing).runWith(Sink.seq)

    firstElem.success(Some(0))
    f1.futureValue should ===(0 to 18 by 2)
    f2.futureValue should ===(1 to 19 by 2)

  }

  it should "properly signal error to consumers" in assertAllStagesStopped {
    val upstream = TestPublisher.probe[Int]()
    val source = Source.fromPublisher(upstream).runWith(
      PartitionHubWithBroadcast.sink((size, elem) ⇒ elem % size, startAfterNrOfConsumers = 2, bufferSize = 8))

    val downstream1 = TestSubscriber.probe[Int]()
    source.runWith(Sink.fromSubscriber(downstream1))
    val downstream2 = TestSubscriber.probe[Int]()
    source.runWith(Sink.fromSubscriber(downstream2))

    downstream1.request(4)
    downstream2.request(8)

    (0 until 16) foreach (upstream.sendNext(_))

    downstream1.expectNext(0, 2, 4, 6)
    downstream2.expectNext(1, 3, 5, 7, 9, 11, 13, 15)

    downstream1.expectNoMessage(100.millis)
    downstream2.expectNoMessage(100.millis)

    upstream.sendError(TE("Failed"))

    downstream1.expectError(TE("Failed"))
    downstream2.expectError(TE("Failed"))
  }

  it should "properly signal completion to consumers arriving after producer finished" in assertAllStagesStopped {
    val source = Source.empty[Int].runWith(PartitionHubWithBroadcast.sink((size, elem) ⇒ elem % size, startAfterNrOfConsumers = 0))
    // Wait enough so the Hub gets the completion. This is racy, but this is fine because both
    // cases should work in the end
    //Thread.sleep(10)

    source.runWith(Sink.seq).futureValue should ===(Nil)
  }

  it should "remember completion for materialisations after completion" in {

    val (sourceProbe, source) = TestSource.probe[Unit].toMat(
      PartitionHubWithBroadcast.sink((size, elem) ⇒ 0, startAfterNrOfConsumers = 0))(Keep.both).run()
    val sinkProbe = source.runWith(TestSink.probe[Unit])

    sourceProbe.sendComplete()

    sinkProbe.request(1)
    sinkProbe.expectComplete()

    // Materialize a second time. There was a race here, where we managed to enqueue our Source registration just
    // immediately before the Hub shut down.
    val sink2Probe = source.runWith(TestSink.probe[Unit])

    sink2Probe.request(1)
    sink2Probe.expectComplete()
  }

  it should "properly signal error to consumers arriving after producer finished" in assertAllStagesStopped {
    val source = Source.failed[Int](TE("Fail!")).runWith(
      PartitionHubWithBroadcast.sink((size, elem) ⇒ 0, startAfterNrOfConsumers = 0))
    // Wait enough so the Hub gets the failure. This is racy, but this is fine because both
    // cases should work in the end
    //Thread.sleep(10)

    a[TE] shouldBe thrownBy {
      Await.result(source.runWith(Sink.seq), 3.seconds)
    }
  }

  it should "drop elements with an index less than -1" in assertAllStagesStopped {
    val source = Source(0 until 10).runWith(PartitionHubWithBroadcast.sink(
      (size, elem) ⇒ if (elem == 3 || elem == 4) -2 else elem % size, startAfterNrOfConsumers = 2, bufferSize = 8))
    val result1 = source.runWith(Sink.seq)
    val result2 = source.runWith(Sink.seq)
    result1.futureValue should ===((0 to 8 by 2).filterNot(_ == 4))
    result2.futureValue should ===((1 to 9 by 2).filterNot(_ == 3))
  }

  // broadcast tests

  it should "broadcast in the happy case" in assertAllStagesStopped {
    val source = Source(1 to 10).runWith(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 1, bufferSize = 8))
    source.runWith(Sink.seq).futureValue should ===(1 to 10)
  }

  it should "broadcast the same elements to consumers attaching around the same time" in assertAllStagesStopped {
    val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 10))
      .toMat(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 2, bufferSize = 8))(Keep.both)
      .run()

    val f1 = source.runWith(Sink.seq)
    val f2 = source.runWith(Sink.seq)

    firstElem.success(Some(1))
    f1.futureValue should ===(1 to 10)
    f2.futureValue should ===(1 to 10)
  }

  it should "broadcast the same prefix to consumers attaching around the same time if one cancels earlier" in assertAllStagesStopped {
    val (firstElem, source) = Source.maybe[Int]
      .concat(Source(2 to 20)).toMat(
        PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 2, bufferSize = 8)
      )(Keep.both).run()

    val f1 = source.runWith(Sink.seq)
    val f2 = source.take(10).runWith(Sink.seq)

    firstElem.success(Some(1))
    f1.futureValue should ===(1 to 20)
    f2.futureValue should ===(1 to 10)
  }

  // TODO: JMM: we don't need this requirement for our usecase, revisit later.
/*
  it should "ensure that subsequent consumers see subsequent elements without gap" in assertAllStagesStopped {
    val source = Source(1 to 20).runWith(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 1, bufferSize = 8))
    source.take(10).runWith(Sink.seq).futureValue should ===(1 to 10)
    source.take(10).runWith(Sink.seq).futureValue should ===(11 to 20)
  }
*/

  it should "send the same elements to consumers of different speed attaching around the same time" in assertAllStagesStopped {
    val (firstElem, source) = Source.maybe[Int]
      .concat(Source(2 to 10)).toMat(
        PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 2, bufferSize = 8)
      )(Keep.both).run()

    val f1 = source.throttle(1, 10.millis, 3, ThrottleMode.shaping).runWith(Sink.seq)
    val f2 = source.runWith(Sink.seq)

    firstElem.success(Some(1))
    f1.futureValue should ===(1 to 10)
    f2.futureValue should ===(1 to 10)
  }

  it should "send the same elements to consumers of attaching around the same time if the producer is slow" in assertAllStagesStopped {
    val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 10))
      .throttle(1, 10.millis, 3, ThrottleMode.shaping)
      .toMat(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 1, bufferSize = 8))(Keep.both).run()

    val (future1, f1) = source.toMat(Sink.seq)(Keep.both).run()
    val (future2, f2) = source.toMat(Sink.seq)(Keep.both).run()

    Await.ready(future1, 10.millis)
    Await.ready(future2, 10.millis)

    firstElem.success(Some(1))
    f1.futureValue should ===(1 to 10)
    f2.futureValue should ===(1 to 10)
  }

  // @todo DemandThreshold check had to be removed to get this to work. investigate why.
  it should "ensure that from two different speed consumers the slower controls the broadcast rate" in assertAllStagesStopped {
    val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 20)).toMat(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 2, bufferSize = 1))(Keep.both).run()

    val f1 = source.throttle(1, 10.millis, 1, ThrottleMode.shaping).runWith(Sink.seq)
    // Second cannot be overwhelmed since the first one throttles the overall rate, and second allows a higher rate
    val f2 = source.throttle(10, 10.millis, 8, ThrottleMode.enforcing).runWith(Sink.seq)

    firstElem.success(Some(1))

    f1.futureValue should ===(1 to 20)
    f2.futureValue should ===(1 to 20)

  }


  it should "broadcast the same elements to consumers attaching around the same time with a buffer size of one" in assertAllStagesStopped {
    val (firstElem, source) = Source.maybe[Int].concat(Source(2 to 10))
      .toMat(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 2, bufferSize = 1))(Keep.both).run()

    val f1 = source.runWith(Sink.seq)
    val f2 = source.runWith(Sink.seq)

    firstElem.success(Some(1))
    f1.futureValue should ===(1 to 10)
    f2.futureValue should ===(1 to 10)
  }

  it should "be able to implement a keep-dropping-if-unsubscribed policy with a simple Sink.ignore" in assertAllStagesStopped {
    val killSwitch = KillSwitches.shared("test-switch")
    val source = Source.fromIterator(() ⇒ Iterator.from(0)).via(killSwitch.flow)
      .runWith(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 1, bufferSize = 8))

    // Now the Hub "drops" elements until we attach a new consumer (Source.ignore consumes as fast as possible)
    source.runWith(Sink.ignore)

    // Now we attached a subscriber which will block the Sink.ignore to "take away" and drop elements anymore,
    // turning the PartitionHubWithBroadcast to a normal non-dropping mode
    val downstream = TestSubscriber.probe[Int]()
    source.runWith(Sink.fromSubscriber(downstream))

    downstream.request(1)
    val first = downstream.expectNext()

    for (i ← (first + 1) to (first + 10)) {
      downstream.request(1)
      downstream.expectNext(i)
    }

    downstream.cancel()

    killSwitch.shutdown()
  }

  it should "properly signal broadcast error to consumers" in assertAllStagesStopped {
    val upstream = TestPublisher.probe[Int]()
    val source = Source.fromPublisher(upstream)
      .runWith(PartitionHubWithBroadcast.sink((size, elem) => -1, startAfterNrOfConsumers = 2, bufferSize = 8))

    val downstream1 = TestSubscriber.probe[Int]()
    val downstream2 = TestSubscriber.probe[Int]()
    source.runWith(Sink.fromSubscriber(downstream1))
    source.runWith(Sink.fromSubscriber(downstream2))

    downstream1.request(4)
    downstream2.request(8)

    (1 to 8) foreach upstream.sendNext

    downstream1.expectNext(1, 2, 3, 4)
    downstream2.expectNext(1, 2, 3, 4, 5, 6, 7, 8)

    downstream1.expectNoMessage(100.millis)
    downstream2.expectNoMessage(100.millis)

    upstream.sendError(TE("Failed"))

    downstream1.expectError(TE("Failed"))
    downstream2.expectError(TE("Failed"))
  }
}
