/**
  * Copyright (C) 2015-2018 Lightbend Inc. <https://www.lightbend.com>
  * Copyright (C) 2018 Joseph Malicki
  */

package com.natomatrading.util.akka

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.dispatch.AbstractNodeQueue
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage._
import scala.annotation.tailrec
import akka.stream.Attributes.LogLevels

/**
 * A MergeHubWithComplete is a special streaming hub that is able to collect streamed elements from a dynamic set of
 * producers. It consists of two parts, a [[Source]] and a [[Sink]]. The [[Source]] streams the element to a consumer from
 * its merged inputs. Once the consumer has been materialized, the [[Source]] returns a materialized value which is
 * the corresponding [[Sink]]. This [[Sink]] can then be materialized arbitrary many times, where each of the new
 * materializations will feed its consumed elements to the original [[Source]].
 */
object MergeHubWithComplete {
  private val Cancel = -1

  /**
   * Creates a [[Source]] that emits elements merged from a dynamic set of producers. After the [[Source]] returned
   * by this method is materialized, it returns a [[Sink]] as a materialized value. This [[Sink]] can be materialized
   * arbitrary many times and each of the materializations will feed the elements into the original [[Source]].
   *
   * Every new materialization of the [[Source]] results in a new, independent hub, which materializes to its own
   * [[Sink]] for feeding that materialization.
   *
   * Completed or failed [[Sink]]s are simply removed. Once the [[Source]] is cancelled, the Hub is considered closed
   * and any new producers using the [[Sink]] will be cancelled.
   *
   * @param perProducerBufferSize Buffer space used per producer. Default value is 16.
   */
  def source[T](perProducerBufferSize: Int): Source[T, Sink[T, NotUsed]] =
    Source.fromGraph(new MergeHubWithComplete[T](perProducerBufferSize))

  /**
   * Creates a [[Source]] that emits elements merged from a dynamic set of producers. After the [[Source]] returned
   * by this method is materialized, it returns a [[Sink]] as a materialized value. This [[Sink]] can be materialized
   * arbitrary many times and each of the materializations will feed the elements into the original [[Source]].
   *
   * Every new materialization of the [[Source]] results in a new, independent hub, which materializes to its own
   * [[Sink]] for feeding that materialization.
   *
   * Completed or failed [[Sink]]s are simply removed. Once the [[Source]] is cancelled, the Hub is considered closed
   * and any new producers using the [[Sink]] will be cancelled.
   */
  def source[T]: Source[T, Sink[T, NotUsed]] = source(perProducerBufferSize = 16)

  final class ProducerFailed(msg: String, cause: Throwable) extends RuntimeException(msg, cause)
}

/**
 * INTERNAL API
 */
private[akka] class MergeHubWithComplete[T](perProducerBufferSize: Int, completeOnLastPublisherExit: Boolean = false) extends GraphStageWithMaterializedValue[SourceShape[T], Sink[T, NotUsed]] {
  require(perProducerBufferSize > 0, "Buffer size must be positive")

  val out: Outlet[T] = Outlet("MergeHubWithComplete.out")
  override val shape: SourceShape[T] = SourceShape(out)

  // Half of buffer size, rounded up
  private[this] val DemandThreshold = (perProducerBufferSize / 2) + (perProducerBufferSize % 2)

  private sealed trait Event {
    def id: Long
  }

  private final case class Element(id: Long, elem: T) extends Event
  private final case class Register(id: Long, demandCallback: AsyncCallback[Long]) extends Event
  private final case class Deregister(id: Long) extends Event

  final class InputState(signalDemand: AsyncCallback[Long]) {
    private var untilNextDemandSignal = DemandThreshold

    def onElement(): Unit = {
      untilNextDemandSignal -= 1
      if (untilNextDemandSignal == 0) {
        untilNextDemandSignal = DemandThreshold
        signalDemand.invoke(DemandThreshold)
      }
    }

    def close(): Unit = signalDemand.invoke(MergeHubWithComplete.Cancel)

  }

  final class MergedSourceLogic(_shape: Shape, producerCount: AtomicLong) extends GraphStageLogic(_shape) with OutHandler {
    /*
     * Basically all merged messages are shared in this queue. Individual buffer sizes are enforced by tracking
     * demand per producer in the 'demands' Map. One twist here is that the same queue contains control messages,
     * too. Since the queue is read only if the output port has been pulled, downstream backpressure can delay
     * processing of control messages. This causes no issues though, see the explanation in 'tryProcessNext'.
     */
    private val queue = new AbstractNodeQueue[Event] {}
    @volatile private[this] var needWakeup = false
    @volatile private[this] var shuttingDown = false

    private[this] val demands = scala.collection.mutable.LongMap.empty[InputState]
    private[this] val wakeupCallback = getAsyncCallback[NotUsed]((_) =>
      // We are only allowed to dequeue if we are not backpressured. See comment in tryProcessNext() for details.
      if (isAvailable(out)) tryProcessNext(firstAttempt = true))

    setHandler(out, this)

    // Returns true when we have not consumed demand, false otherwise
    private def onEvent(ev: Event): Boolean = ev match {
      case Element(id, elem) =>
        demands(id).onElement()
        push(out, elem)
        false
      case Register(id, callback) =>
        demands.put(id, new InputState(callback))
        true
      case Deregister(id) =>
        demands.remove(id)
        if (demands.isEmpty) {
          shuttingDown = true
          complete(out)
        }
        true
    }

    override def onPull(): Unit = tryProcessNext(firstAttempt = true)

    @tailrec private def tryProcessNext(firstAttempt: Boolean): Unit = {
      val nextElem = queue.poll()

      // That we dequeue elements from the queue when there is demand means that Register and Deregister messages
      // might be delayed for arbitrary long. This is not a problem as Register is only interesting if it is followed
      // by actual elements, which would be delayed anyway by the backpressure.
      // Unregister is only used to keep the map growing too large, but otherwise it is not critical to process it
      // timely. In fact, the only way the map could keep growing would mean that we dequeue Registers from the
      // queue, but then we will eventually reach the Deregister message, too.
      if (nextElem ne null) {
        needWakeup = false
        if (onEvent(nextElem)) tryProcessNext(firstAttempt = true)
      } else {
        needWakeup = true
        // additional poll() to grab any elements that might missed the needWakeup
        // and have been enqueued just after it
        if (firstAttempt)
          tryProcessNext(firstAttempt = false)
      }
    }

    def isShuttingDown: Boolean = shuttingDown

    // External API
    def enqueue(ev: Event): Unit = {
      queue.add(ev)
      /*
       * Simple volatile var is enough, there is no need for a CAS here. The first important thing to note
       * that we don't care about double-wakeups. Since the "wakeup" is actually handled by an actor message
       * (AsyncCallback) we don't need to handle this case, a double-wakeup will be idempotent (only wasting some cycles).
       *
       * The only case that we care about is a missed wakeup. The characteristics of a missed wakeup are the following:
       *  (1) there is at least one message in the queue
       *  (2) the consumer is not running right now
       *  (3) no wakeupCallbacks are pending
       *  (4) all producers exited this method
       *
       * From the above we can deduce that
       *  (5) needWakeup = true at some point in time. This is implied by (1) and (2) and the
       *      'tryProcessNext' method
       *  (6) There must have been one producer that observed needWakeup = false. This follows from (4) and (3)
       *      and the implementation of this method. In addition, this producer arrived after needWakeup = true,
       *      since before that, every queued elements have been consumed.
       *  (7) There have been at least one producer that observed needWakeup = true and enqueued an element and
       *      a wakeup signal. This follows from (5) and (6), and the fact that either this method sets
       *      needWakeup = false, or the 'tryProcessNext' method, i.e. a wakeup must happened since (5)
       *  (8) If there were multiple producers satisfying (6) take the last one. Due to (6), (3) and (4) we know
       *      there cannot be a wakeup pending, and we just enqueued an element, so (1) holds. Since we are the last
       *      one, (2) must be true or there is no lost wakeup. However, due to (7) we know there was at least one
       *      wakeup (otherwise needWakeup = true). Now, if the consumer is still running (2) is violated,
       *      if not running then needWakeup = false is violated (which comes from (6)). No matter what,
       *      contradiction. QED.
       *
       */
      if (needWakeup) {
        needWakeup = false
        wakeupCallback.invoke(NotUsed)
      }
    }

    override def postStop(): Unit = {
      // First announce that we are shutting down. This will notify late-comers to not even put anything in the queue
      shuttingDown = true
      // Anybody that missed the announcement needs to be notified.
      var event = queue.poll()
      while (event ne null) {
        event match {
          case Register(_, demandCallback) => demandCallback.invoke(MergeHubWithComplete.Cancel)
          case _                           =>
        }
        event = queue.poll()
      }

      // Kill everyone else
      val states = demands.valuesIterator
      while (states.hasNext) {
        states.next().close()
      }
    }
  }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Sink[T, NotUsed]) = {
    val idCounter = new AtomicLong()

    val logic: MergedSourceLogic = new MergedSourceLogic(shape, idCounter)

    val sink = new GraphStage[SinkShape[T]] {
      val in: Inlet[T] = Inlet("MergeHubWithComplete.in")
      override val shape: SinkShape[T] = SinkShape(in)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler {
        // Start from non-zero demand to avoid initial delays.
        // The HUB will expect this behavior.
        private[this] var demand: Long = perProducerBufferSize
        private[this] val id = idCounter.getAndIncrement()

        override def preStart(): Unit = {
          if (!logic.isShuttingDown) {
            logic.enqueue(Register(id, getAsyncCallback(onDemand)))

            // At this point, we could be in the unfortunate situation that:
            // - we missed the shutdown announcement and entered this arm of the if statement
            // - *before* we enqueued our Register event, the Hub already finished looking at the queue
            //   and is now dead, so we are never notified again.
            // To safeguard against this, we MUST check the announcement again. This is enough:
            // if the Hub is no longer looking at the queue, then it must be that isShuttingDown must be already true.
            if (!logic.isShuttingDown) pullWithDemand()
            else completeStage()
          } else {
            completeStage()
          }
        }
        override def postStop(): Unit = {
          // Unlike in the case of preStart, we don't care about the Hub no longer looking at the queue.
          if (!logic.isShuttingDown) logic.enqueue(Deregister(id))
        }

        override def onPush(): Unit = {
          logic.enqueue(Element(id, grab(in)))
          if (demand > 0) pullWithDemand()
        }

        private def pullWithDemand(): Unit = {
          demand -= 1
          pull(in)
        }

        // Make some noise
        override def onUpstreamFailure(ex: Throwable): Unit = {
          throw new MergeHubWithComplete.ProducerFailed("Upstream producer failed with exception, " +
            "removing from MergeHub now", ex)
        }

        private def onDemand(moreDemand: Long): Unit = {
          if (moreDemand == MergeHubWithComplete.Cancel) completeStage()
          else {
            demand += moreDemand
            if (!hasBeenPulled(in)) pullWithDemand()
          }
        }

        setHandler(in, this)
      }

    }

    // propagate LogLevels attribute so that MergeHub can be used with onFailure = LogLevels.Off
    val sinkWithAttributes = inheritedAttributes.get[LogLevels] match {
      case Some(a) => Sink.fromGraph(sink).addAttributes(Attributes(a))
      case None    => Sink.fromGraph(sink)
    }

    (logic, sinkWithAttributes)
  }
}
