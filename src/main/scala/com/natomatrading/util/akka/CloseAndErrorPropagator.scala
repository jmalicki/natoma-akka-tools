package com.natomatrading.util.akka

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}


/**
  * GraphStage that only propagates completion and error, not any data.
  * This allows coupling dynamic handlers like MergeHub and PartitionHub together
  * through the dynamic ends, so that the upstream of PartitionHub can propagate
  * errors and completion downstream beyond MergeHub.
  *
  * @tparam In Type of input (all input is ignored)
  * @tparam Out Type of output (no output is ever generated)
  */
class CloseAndErrorPropagator[In, Out] extends GraphStage[FlowShape[In, Out]] {

  val in = Inlet[In]("CloseAndErrorPropagator.in")
  val out = Outlet[Out]("CloseAndErrorPropagator.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandlers(in, out, new InHandler with OutHandler {
        override def onPull(): Unit = pull(in)

        // ignore all input, just ask for more when it comes.
        override def onPush(): Unit = {
          grab(in)
          pull(in)
        }
      })
    }
}
