package com.natomatrading.util.akka

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference, AtomicReferenceArray}

import akka.NotUsed
import akka.annotation.{ApiMayChange, DoNotInherit, InternalApi}
import akka.stream._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.Queue
import scala.collection.mutable.LongMap
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

// Forked from https://github.com/akka/akka/blob/master/akka-stream/src/main/scala/akka/stream/scaladsl/Hub.scala


/**
  * A `PartitionHub` is a special streaming hub that is able to route streamed elements to a dynamic set of consumers.
  * It consists of two parts, a [[Sink]] and a [[Source]]. The [[Sink]] e elements from a producer to the
  * actually live consumers it has. The selection of consumer is done with a function. Each element can be routed to
  * only one consumer. Once the producer has been materialized, the [[Sink]] it feeds into returns a
  * materialized value which is the corresponding [[Source]]. This [[Source]] can be materialized an arbitrary number
  * of times, where each of the new materializations will receive their elements from the original [[Sink]].
  */
object PartitionHubWithBroadcast {

  /**
    * INTERNAL API
    */
  @InternalApi private[akka] val defaultBufferSize = 256

  /**
    * Creates a [[Sink]] that receives elements from its upstream producer and routes them to a dynamic set
    * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
    * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
    * elements from the original [[Sink]].
    *
    * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
    * [[Source]] for consuming the [[Sink]] of that materialization.
    *
    * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
    * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
    * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
    * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
    * cancelled are simply removed from the dynamic set of consumers.
    *
    * This `statefulSink` should be used when there is a need to keep mutable state in the partition function,
    * e.g. for implemening round-robin or sticky session kind of routing. If state is not needed the [[#sink]] can
    * be more convenient to use.
    *
    * @param partitioner Function that decides where to route an element. It is a factory of a function to
    *   to be able to hold stateful variables that are unique for each materialization. The function
    *   takes two parameters; the first is information about active consumers, including an array of consumer
    *   identifiers and the second is the stream element. The function should return the selected consumer
    *   identifier for the given element. The function will never be called when there are no active consumers,
    *   i.e. there is always at least one element in the array of identifiers.
    * @param startAfterNrOfConsumers Elements are buffered until this number of consumers have been connected.
    *   This is only used initially when the operator is starting up, i.e. it is not honored when consumers have
    *   been removed (canceled).
    * @param bufferSize Total number of elements that can be buffered. If this buffer is full, the producer
    *   is backpressured.
    */
  @ApiMayChange def statefulSink[T](partitioner: () ⇒ (ConsumerInfo, T) ⇒ Long, startAfterNrOfConsumers: Int,
                                    bufferSize: Int = defaultBufferSize): Sink[T, Source[T, NotUsed]] =
    Sink.fromGraph(new PartitionHubWithBroadcast[T](partitioner, startAfterNrOfConsumers, bufferSize))

  /**
    * Creates a [[Sink]] that receives elements from its upstream producer and routes them to a dynamic set
    * of consumers. After the [[Sink]] returned by this method is materialized, it returns a [[Source]] as materialized
    * value. This [[Source]] can be materialized an arbitrary number of times and each materialization will receive the
    * elements from the original [[Sink]].
    *
    * Every new materialization of the [[Sink]] results in a new, independent hub, which materializes to its own
    * [[Source]] for consuming the [[Sink]] of that materialization.
    *
    * If the original [[Sink]] is failed, then the failure is immediately propagated to all of its materialized
    * [[Source]]s (possibly jumping over already buffered elements). If the original [[Sink]] is completed, then
    * all corresponding [[Source]]s are completed. Both failure and normal completion is "remembered" and later
    * materializations of the [[Source]] will see the same (failure or completion) state. [[Source]]s that are
    * cancelled are simply removed from the dynamic set of consumers.
    *
    * This `sink` should be used when the routing function is stateless, e.g. based on a hashed value of the
    * elements. Otherwise the [[#statefulSink]] can be used to implement more advanced routing logic.
    *
    * @param partitioner Function that decides where to route an element. The function takes two parameters;
    *   the first is the number of active consumers and the second is the stream element. The function should
    *   return the index of the selected consumer for the given element, i.e. int greater than or equal to 0
    *   and less than number of consumers. E.g. `(size, elem) => math.abs(elem.hashCode % size)`. It's also
    *   possible to use `-1` to drop the element.
    * @param startAfterNrOfConsumers Elements are buffered until this number of consumers have been connected.
    *   This is only used initially when the operator is starting up, i.e. it is not honored when consumers have
    *   been removed (canceled).
    * @param bufferSize Total number of elements that can be buffered. If this buffer is full, the producer
    *   is backpressured.
    */
  @ApiMayChange
  def sink[T](partitioner: (Int, T) ⇒ Int, startAfterNrOfConsumers: Int,
              bufferSize: Int = defaultBufferSize): Sink[T, Source[T, NotUsed]] = {
    val fun: (ConsumerInfo, T) ⇒ Long = { (info, elem) ⇒
      val idx = partitioner(info.size, elem)
      if (idx < 0) -1L
      else info.consumerIdByIdx(idx)
    }
    statefulSink(() ⇒ fun, startAfterNrOfConsumers, bufferSize)
  }

  @DoNotInherit @ApiMayChange trait ConsumerInfo {

    /**
      * Sequence of all identifiers of current consumers.
      *
      * Use this method only if you need to enumerate consumer existing ids.
      * When selecting a specific consumerId by its index, prefer using the dedicated [[#consumerIdByIdx]] method instead,
      * which is optimised for this use case.
      */
    def consumerIds: immutable.IndexedSeq[Long]

    def getConsumerIds: java.util.List[Long]

    /** Obtain consumer identifier by index */
    def consumerIdByIdx(idx: Int): Long

    /**
      * Approximate number of buffered elements for a consumer.
      * Larger value than other consumers could be an indication of
      * that the consumer is slow.
      *
      * Note that this is a moving target since the elements are
      * consumed concurrently.
      */
    def queueSize(consumerId: Long): Int

    /**
      * Number of attached consumers.
      */
    def size: Int

  }

  /**
    * INTERNAL API
    */
  @InternalApi private[akka] object Internal {
    sealed trait ConsumerEvent
    case object Wakeup extends ConsumerEvent
    final case class HubCompleted(failure: Option[Throwable]) extends ConsumerEvent
    case object Initialize extends ConsumerEvent

    sealed trait HubEvent
    case object RegistrationPending extends HubEvent
    final case class UnRegister(id: Long) extends HubEvent
    final case class NeedWakeup(consumer: Consumer) extends HubEvent
    final case class Consumer(id: Long, callback: AsyncCallback[ConsumerEvent])
    case object TryPull extends HubEvent

    case object Completed

    sealed trait HubState
    final case class Open(callbackFuture: Future[AsyncCallback[HubEvent]], registrations: List[Consumer]) extends HubState
    final case class Closed(failure: Option[Throwable]) extends HubState

    // The reason for the two implementations here is that the common case (as I see it) is to have a few (< 100)
    // consumers over the lifetime of the hub but we must of course also support more.
    // FixedQueues is more efficient than ConcurrentHashMap so we use that for the first 128 consumers.
    private val FixedQueues = 128

    // Need the queue to be pluggable to be able to use a more performant (less general)
    // queue in Artery
    trait PartitionQueue {
      def init(id: Long): Unit
      def totalSize: Int
      def size(id: Long): Int
      def isEmpty(id: Long): Boolean
      def nonEmpty(id: Long): Boolean
      def offer(id: Long, elem: Any): Unit
      def poll(id: Long): AnyRef
      def remove(id: Long): Unit
    }

    object ConsumerQueue {
      val empty = ConsumerQueue(Queue.empty, 0)
    }

    final case class ConsumerQueue(queue: Queue[Any], size: Int) {
      def enqueue(elem: Any): ConsumerQueue =
        new ConsumerQueue(queue.enqueue(elem), size + 1)

      def isEmpty: Boolean = size == 0

      def head: Any = queue.head

      def tail: ConsumerQueue =
        new ConsumerQueue(queue.tail, size - 1)
    }

    class PartitionQueueImpl extends PartitionQueue {
      private val queues1 = new AtomicReferenceArray[ConsumerQueue](FixedQueues)
      private val queues2 = new ConcurrentHashMap[Long, ConsumerQueue]
      private val _totalSize = new AtomicInteger

      override def init(id: Long): Unit = {
        if (id < FixedQueues)
          queues1.set(id.toInt, ConsumerQueue.empty)
        else
          queues2.put(id, ConsumerQueue.empty)
      }

      override def totalSize: Int = _totalSize.get

      def size(id: Long): Int = {
        val queue =
          if (id < FixedQueues) queues1.get(id.toInt)
          else queues2.get(id)
        if (queue eq null)
          throw new IllegalArgumentException(s"Invalid stream identifier: $id")
        queue.size
      }

      override def isEmpty(id: Long): Boolean = {
        val queue =
          if (id < FixedQueues) queues1.get(id.toInt)
          else queues2.get(id)
        if (queue eq null)
          throw new IllegalArgumentException(s"Invalid stream identifier: $id")
        queue.isEmpty
      }

      override def nonEmpty(id: Long): Boolean = !isEmpty(id)

      override def offer(id: Long, elem: Any): Unit = {
        @tailrec def offer1(): Unit = {
          val i = id.toInt
          val queue = queues1.get(i)
          if (queue eq null)
            throw new IllegalArgumentException(s"Invalid stream identifier: $id")
          if (queues1.compareAndSet(i, queue, queue.enqueue(elem)))
            _totalSize.incrementAndGet()
          else
            offer1() // CAS failed, retry
        }

        @tailrec def offer2(): Unit = {
          val queue = queues2.get(id)
          if (queue eq null)
            throw new IllegalArgumentException(s"Invalid stream identifier: $id")
          if (queues2.replace(id, queue, queue.enqueue(elem))) {
            _totalSize.incrementAndGet()
          } else
            offer2() // CAS failed, retry
        }

        def publish(): Unit = {
          for (id <- 0 until FixedQueues) {
            if (queues2.contains(id))
              offer(id, elem)
          }
          for (id <- queues2.keySet.asScala)
            offer(id, elem)
        }

        if (id < 0) publish() else if (id < FixedQueues) offer1() else offer2()
      }

      override def poll(id: Long): AnyRef = {
        @tailrec def poll1(): AnyRef = {
          val i = id.toInt
          val queue = queues1.get(i)
          if ((queue eq null) || queue.isEmpty) null
          else if (queues1.compareAndSet(i, queue, queue.tail)) {
            _totalSize.decrementAndGet()
            queue.head.asInstanceOf[AnyRef]
          } else
            poll1() // CAS failed, try again
        }

        @tailrec def poll2(): AnyRef = {
          val queue = queues2.get(id)
          if ((queue eq null) || queue.isEmpty) null
          else if (queues2.replace(id, queue, queue.tail)) {
            _totalSize.decrementAndGet()
            queue.head.asInstanceOf[AnyRef]
          } else
            poll2() // CAS failed, try again
        }

        if (id < FixedQueues) poll1() else poll2()
      }

      override def remove(id: Long): Unit = {
        (if (id < FixedQueues) queues1.getAndSet(id.toInt, null)
        else queues2.remove(id)) match {
          case null  ⇒
          case queue ⇒ _totalSize.addAndGet(-queue.size)
        }
      }

    }
  }
}

/**
  * INTERNAL API
  */
@InternalApi private[akka] class PartitionHubWithBroadcast[T](
                                                  partitioner:             () ⇒ (PartitionHubWithBroadcast.ConsumerInfo, T) ⇒ Long,
                                                  startAfterNrOfConsumers: Int, bufferSize: Int)
  extends GraphStageWithMaterializedValue[SinkShape[T], Source[T, NotUsed]] {
  import PartitionHubWithBroadcast.Internal._
  import PartitionHubWithBroadcast.ConsumerInfo

  val in: Inlet[T] = Inlet("PartitionHubWithBroadcast.in")
  override val shape: SinkShape[T] = SinkShape(in)

  // Need the queue to be pluggable to be able to use a more performant (less general)
  // queue in Artery
  def createQueue(): PartitionQueue = new PartitionQueueImpl

  private class PartitionSinkLogic(_shape: Shape)
    extends GraphStageLogic(_shape) with InHandler {

    // Half of buffer size, rounded up
    private val DemandThreshold = (bufferSize / 2) + (bufferSize % 2)

    private val materializedPartitioner = partitioner()

    private val callbackPromise: Promise[AsyncCallback[HubEvent]] = Promise()
    private val noRegistrationsState = Open(callbackPromise.future, Nil)
    val state = new AtomicReference[HubState](noRegistrationsState)
    private var initialized = false

    private val queue = createQueue()
    private var pending = Vector.empty[T]
    private var consumerInfo: ConsumerInfoImpl = new ConsumerInfoImpl(Vector.empty)
    private val needWakeup: LongMap[Consumer] = LongMap.empty

    private var callbackCount = 0L

    private final class ConsumerInfoImpl(val consumers: Vector[Consumer])
      extends ConsumerInfo { info ⇒

      override def queueSize(consumerId: Long): Int =
        queue.size(consumerId)

      override def size: Int = consumers.size

      override def consumerIds: immutable.IndexedSeq[Long] =
        consumers.map(_.id)

      override def consumerIdByIdx(idx: Int): Long =
        consumers(idx).id

      override def getConsumerIds: java.util.List[Long] =
        new util.AbstractList[Long] {
          override def get(idx: Int): Long = info.consumerIdByIdx(idx)
          override def size(): Int = info.size
        }
    }

    override def preStart(): Unit = {
      setKeepGoing(true)
      callbackPromise.success(getAsyncCallback[HubEvent](onEvent))
      if (startAfterNrOfConsumers == 0)
        pull(in)
    }

    override def onPush(): Unit = {
      publish(grab(in))
      if (!isFull) pull(in)
    }

    private def isFull: Boolean = {
      (queue.totalSize + pending.size) >= bufferSize
    }

    private def publish(elem: T): Unit = {
      if (!initialized || consumerInfo.consumers.isEmpty) {
        // will be published when first consumers are registered
        pending :+= elem
      } else {
        val id = materializedPartitioner(consumerInfo, elem)
        if (id >= 0) { // negative id is a way to drop the element
          queue.offer(id, elem)
          wakeup(id)
        }
      }
    }

    private def wakeup(id: Long): Unit = {
      needWakeup.get(id) match {
        case None ⇒ // ignore
        case Some(consumer) ⇒
          needWakeup -= id
          consumer.callback.invoke(Wakeup)
      }
    }

    override def onUpstreamFinish(): Unit = {
      if (consumerInfo.consumers.isEmpty)
        completeStage()
      else {
        consumerInfo.consumers.foreach(c ⇒ complete(c.id))
      }
    }

    private def complete(id: Long): Unit = {
      queue.offer(id, Completed)
      wakeup(id)
    }

    private def tryPull(): Unit = {
      if (initialized && !isClosed(in) && !hasBeenPulled(in) && !isFull)
        pull(in)
    }

    private def onEvent(ev: HubEvent): Unit = {
      callbackCount += 1
      ev match {
        case NeedWakeup(consumer) ⇒
          // Also check if the consumer is now unblocked since we published an element since it went asleep.
          if (queue.nonEmpty(consumer.id))
            consumer.callback.invoke(Wakeup)
          else {
            needWakeup.update(consumer.id, consumer)
            tryPull()
          }

        case TryPull ⇒
          tryPull()

        case RegistrationPending ⇒
          state.getAndSet(noRegistrationsState).asInstanceOf[Open].registrations foreach { consumer ⇒
            val newConsumers = (consumerInfo.consumers :+ consumer).sortBy(_.id)
            consumerInfo = new ConsumerInfoImpl(newConsumers)
            queue.init(consumer.id)
            if (newConsumers.size >= startAfterNrOfConsumers) {
              initialized = true
            }

            consumer.callback.invoke(Initialize)

            if (initialized && pending.nonEmpty) {
              pending.foreach(publish)
              pending = Vector.empty[T]
            }

            tryPull()
          }

        case UnRegister(id) ⇒
          val newConsumers = consumerInfo.consumers.filterNot(_.id == id)
          consumerInfo = new ConsumerInfoImpl(newConsumers)
          queue.remove(id)
          if (newConsumers.isEmpty) {
            if (isClosed(in)) completeStage()
          } else
            tryPull()
      }
    }

    override def onUpstreamFailure(ex: Throwable): Unit = {
      val failMessage = HubCompleted(Some(ex))

      // Notify pending consumers and set tombstone
      state.getAndSet(Closed(Some(ex))).asInstanceOf[Open].registrations foreach { consumer ⇒
        consumer.callback.invoke(failMessage)
      }

      // Notify registered consumers
      consumerInfo.consumers.foreach { consumer ⇒
        consumer.callback.invoke(failMessage)
      }
      failStage(ex)
    }

    override def postStop(): Unit = {
      // Notify pending consumers and set tombstone

      @tailrec def tryClose(): Unit = state.get() match {
        case Closed(_) ⇒ // Already closed, ignore
        case open: Open ⇒
          if (state.compareAndSet(open, Closed(None))) {
            val completedMessage = HubCompleted(None)
            open.registrations foreach { consumer ⇒
              consumer.callback.invoke(completedMessage)
            }
          } else tryClose()
      }

      tryClose()
    }

    // Consumer API
    def poll(id: Long, hubCallback: AsyncCallback[HubEvent]): AnyRef = {
      // try pull via async callback when half full
      // this is racy with other threads doing poll but doesn't matter
      if (queue.totalSize == DemandThreshold)
        hubCallback.invoke(TryPull)

      queue.poll(id)
    }

    setHandler(in, this)
  }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Source[T, NotUsed]) = {
    val idCounter = new AtomicLong

    val logic = new PartitionSinkLogic(shape)

    val source = new GraphStage[SourceShape[T]] {
      val out: Outlet[T] = Outlet("PartitionHubWithBroadcast.out")
      override val shape: SourceShape[T] = SourceShape(out)

      override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
        private val id = idCounter.getAndIncrement()
        private var hubCallback: AsyncCallback[HubEvent] = _
        private val callback = getAsyncCallback(onCommand)
        private val consumer = Consumer(id, callback)

        private var callbackCount = 0L

        override def preStart(): Unit = {
          val onHubReady: Try[AsyncCallback[HubEvent]] ⇒ Unit = {
            case Success(callback) ⇒
              hubCallback = callback
              callback.invoke(RegistrationPending)
              if (isAvailable(out)) onPull()
            case Failure(ex) ⇒
              failStage(ex)
          }

          @tailrec def register(): Unit = {
            logic.state.get() match {
              case Closed(Some(ex)) ⇒ failStage(ex)
              case Closed(None)     ⇒ completeStage()
              case previousState @ Open(callbackFuture, registrations) ⇒
                val newRegistrations = consumer :: registrations
                if (logic.state.compareAndSet(previousState, Open(callbackFuture, newRegistrations))) {
                  callbackFuture.onComplete(getAsyncCallback(onHubReady).invoke)(materializer.executionContext)
                } else register()
            }
          }

          register()

        }

        override def onPull(): Unit = {
          if (hubCallback ne null) {
            val elem = logic.poll(id, hubCallback)

            elem match {
              case null ⇒
                hubCallback.invoke(NeedWakeup(consumer))
              case Completed ⇒
                completeStage()
              case _ ⇒
                push(out, elem.asInstanceOf[T])
            }
          }
        }

        override def postStop(): Unit = {
          if (hubCallback ne null)
            hubCallback.invoke(UnRegister(id))
        }

        private def onCommand(cmd: ConsumerEvent): Unit = {
          callbackCount += 1
          cmd match {
            case HubCompleted(Some(ex)) ⇒ failStage(ex)
            case HubCompleted(None)     ⇒ completeStage()
            case Wakeup ⇒
              if (isAvailable(out)) onPull()
            case Initialize ⇒
              if (isAvailable(out) && (hubCallback ne null)) onPull()
          }
        }

        setHandler(out, this)
      }
    }

    (logic, Source.fromGraph(source))
  }
}
