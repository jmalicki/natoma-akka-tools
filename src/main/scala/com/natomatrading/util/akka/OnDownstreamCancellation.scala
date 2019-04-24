package com.natomatrading.util.akka

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

class OnDownstreamCancellation[T](f: => Unit) extends GraphStage[FlowShape[T, T]] {
  val in = Inlet[T]("OnDownstreamCancellation.in")
  val out = Outlet[T]("OnDownstreamCancellation.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandlers(in, out, new InHandler with OutHandler {
        override def onPush(): Unit = {
          emit(out, grab(in))
        }

        override def onPull(): Unit = pull(in)

        override def onDownstreamFinish(): Unit = {
          if (!isClosed(in))
            f
          super.onDownstreamFinish()
        }
      })
    }
}
