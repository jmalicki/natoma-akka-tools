package com.natomatrading.util.akka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._

class CloseAndErrorPropagatorSpec extends AnyFlatSpec {
  behavior of "CloseAndErrorPropagator"

  implicit val system:ActorSystem = ActorSystem()
  implicit val materializer:ActorMaterializer = ActorMaterializer()

  it should "propagate complete" in {
    val (pub, sub) = TestSource.probe[Int].via(new CloseAndErrorPropagator)
      .toMat(TestSink.probe[Int])(Keep.both).run()

    sub.request(1)
    pub.expectRequest()
    pub.sendComplete()
    sub.expectComplete()
  }

  it should "propagate errors" in {
    val (pub, sub) = TestSource.probe[Int].via(new CloseAndErrorPropagator)
      .toMat(TestSink.probe[Int])(Keep.both).run()

    val error = new Exception("CloseAndErrorPropagator test")

    sub.request(1)
    pub.expectRequest()
    pub.sendError(error)
    sub.expectError(error)
  }

  it should "ignore any input sent before complete" in {
    val (pub, sub) = TestSource.probe[Int].via(new CloseAndErrorPropagator)
      .toMat(TestSink.probe[Int])(Keep.both).run()

    sub.request(1)
    pub.sendNext(0)
    pub.sendNext(1)
    sub.expectNoMessage(10.millis)
    pub.sendComplete()
    sub.expectComplete()
  }
}
