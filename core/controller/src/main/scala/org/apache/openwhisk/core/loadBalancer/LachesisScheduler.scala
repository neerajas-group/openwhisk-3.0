/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import java.util.concurrent.ThreadLocalRandom

// import akka.actor.{Actor, ActorSystem, Cancellable, Props}
import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import org.apache.openwhisk.common.InvokerState.{Healthy, Offline, Unhealthy, Unresponsive}
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.common.LoggingMarkers._
import org.apache.openwhisk.core.controller.Controller
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader

import scala.annotation.tailrec
import scala.concurrent.Future
// import scala.concurrent.duration.FiniteDuration
import scala.collection.mutable
import spray.json._

/**
 * A loadbalancer that schedules workload based on a hashing-algorithm.
 *
 * ## Algorithm
 *
 * At first, for every namespace + action pair a hash is calculated and then an invoker is picked based on that hash
 * (`hash % numInvokers`). The determined index is the so called "home-invoker". This is the invoker where the following
 * progression will **always** start. If this invoker is healthy (see "Invoker health checking") and if there is
 * capacity on that invoker (see "Capacity checking"), the request is scheduled to it.
 *
 * If one of these prerequisites is not true, the index is incremented by a step-size. The step-sizes available are the
 * all coprime numbers smaller than the amount of invokers available (coprime, to minimize collisions while progressing
 * through the invokers). The step-size is picked by the same hash calculated above (`hash & numStepSizes`). The
 * home-invoker-index is now incremented by the step-size and the checks (healthy + capacity) are done on the invoker
 * we land on now.
 *
 * This procedure is repeated until all invokers have been checked at which point the "overload" strategy will be
 * employed, which is to choose a healthy invoker randomly. In a steadily running system, that overload means that there
 * is no capacity on any invoker left to schedule the current request to.
 *
 * If no invokers are available or if there are no healthy invokers in the system, the loadbalancer will return an error
 * stating that no invokers are available to take any work. Requests are not queued anywhere in this case.
 *
 * An example:
 * - availableInvokers: 10 (all healthy)
 * - hash: 13
 * - homeInvoker: hash % availableInvokers = 13 % 10 = 3
 * - stepSizes: 1, 3, 7 (note how 2 and 5 is not part of this because it's not coprime to 10)
 * - stepSizeIndex: hash % numStepSizes = 13 % 3 = 1 => stepSize = 3
 *
 * Progression to check the invokers: 3, 6, 9, 2, 5, 8, 1, 4, 7, 0 --> done
 *
 * This heuristic is based on the assumption, that the chance to get a warm container is the best on the home invoker
 * and degrades the more steps you make. The hashing makes sure that all loadbalancers in a cluster will always pick the
 * same home invoker and do the same progression for a given action.
 *
 * Known caveats:
 * - This assumption is not always true. For instance, two heavy workloads landing on the same invoker can override each
 *   other, which results in many cold starts due to all containers being evicted by the invoker to make space for the
 *   "other" workload respectively. Future work could be to keep a buffer of invokers last scheduled for each action and
 *   to prefer to pick that one. Then the second-last one and so forth.
 *
 * ## Capacity checking
 *
 * The maximum capacity per invoker is configured using `user-memory`, which is the maximum amount of memory of actions
 * running in parallel on that invoker.
 *
 * Spare capacity is determined by what the loadbalancer thinks it scheduled to each invoker. Upon scheduling, an entry
 * is made to update the books and a slot for each MB of the actions memory limit in a Semaphore is taken. These slots
 * are only released after the response from the invoker (active-ack) arrives **or** after the active-ack times out.
 * The Semaphore has as many slots as MBs are configured in `user-memory`.
 *
 * Known caveats:
 * - In an overload scenario, activations are queued directly to the invokers, which makes the active-ack timeout
 *   unpredictable. Timing out active-acks in that case can cause the loadbalancer to prematurely assign new load to an
 *   overloaded invoker, which can cause uneven queues.
 * - The same is true if an invoker is extraordinarily slow in processing activations. The queue on this invoker will
 *   slowly rise if it gets slow to the point of still sending pings, but handling the load so slowly, that the
 *   active-acks time out. The loadbalancer again will think there is capacity, when there is none.
 *
 * Both caveats could be solved in future work by not queueing to invoker topics on overload, but to queue on a
 * centralized overflow topic. Timing out an active-ack can then be seen as a system-error, as described in the
 * following.
 *
 * ## Invoker health checking
 *
 * Invoker health is determined via a kafka-based protocol, where each invoker pings the loadbalancer every second. If
 * no ping is seen for a defined amount of time, the invoker is considered "Offline".
 *
 * Moreover, results from all activations are inspected. If more than 3 out of the last 10 activations contained system
 * errors, the invoker is considered "Unhealthy". If an invoker is unhealthy, no user workload is sent to it, but
 * test-actions are sent by the loadbalancer to check if system errors are still happening. If the
 * system-error-threshold-count in the last 10 activations falls below 3, the invoker is considered "Healthy" again.
 *
 * To summarize:
 * - "Offline": Ping missing for > 10 seconds
 * - "Unhealthy": > 3 **system-errors** in the last 10 activations, pings arriving as usual
 * - "Healthy": < 3 **system-errors** in the last 10 activations, pings arriving as usual
 *
 * ## Horizontal sharding
 *
 * Sharding is employed to avoid both loadbalancers having to share any data, because the metrics used in scheduling
 * are very fast changing.
 *
 * Horizontal sharding means, that each invoker's capacity is evenly divided between the loadbalancers. If an invoker
 * has at most 16 slots available (invoker-busy-threshold = 16), those will be divided to 8 slots for each loadbalancer
 * (if there are 2).
 *
 * If concurrent activation processing is enabled (and concurrency limit is > 1), accounting of containers and
 * concurrency capacity per container will limit the number of concurrent activations routed to the particular
 * slot at an invoker. Default max concurrency is 1.
 *
 * Known caveats:
 * - If a loadbalancer leaves or joins the cluster, all state is removed and created from scratch. Those events should
 *   not happen often.
 * - If concurrent activation processing is enabled, it only accounts for the containers that the current loadbalancer knows.
 *   So the actual number of containers launched at the invoker may be less than is counted at the loadbalancer, since
 *   the invoker may skip container launch in case there is concurrent capacity available for a container launched via
 *   some other loadbalancer.
 */
class LachesisScheduler(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  // Lachesis adding for dummy activation to spin up warm container in background
  private implicit val activationIdFactory = new ActivationId.ActivationIdGenerator {}

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  override protected def emitMetrics() = {
    super.emitMetrics()
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_BLACKBOX,
      schedulingState.blackboxInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(
      INVOKER_TOTALMEM_MANAGED,
      schedulingState.managedInvokers.foldLeft(0L) { (total, curr) =>
        if (curr.status.isUsable) {
          curr.id.userMemory.toMB + total
        } else {
          total
        }
      })
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_MANAGED,
      schedulingState.managedInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_MANAGED, schedulingState.managedInvokers.count(_.status == Offline))
    MetricEmitter.emitGaugeMetric(HEALTHY_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Healthy))
    MetricEmitter.emitGaugeMetric(
      UNHEALTHY_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unhealthy))
    MetricEmitter.emitGaugeMetric(
      UNRESPONSIVE_INVOKER_BLACKBOX,
      schedulingState.blackboxInvokers.count(_.status == Unresponsive))
    MetricEmitter.emitGaugeMetric(OFFLINE_INVOKER_BLACKBOX, schedulingState.blackboxInvokers.count(_.status == Offline))
  }

  /** State needed for scheduling. */
  val schedulingState = LachesisSchedulerState()(lbConfig)

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[LachesisSchedulerState.updateInvokers]] and [[LachesisSchedulerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        // logging.warn(this, s"The newState is: ${newState}")
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  /** Loadbalancer interface methods */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
  override def clusterSize: Int = schedulingState.clusterSize

  /** 1. Publish a message to the loadbalancer */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)

    // Lachesis - we change the hash to hash based on CPU cores and memory allocation of a particular invocation from a function, not just the function
    val hash = LachesisScheduler.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
    val homeInvoker = hash % invokersToUse.size
    val stepSize = stepSizes(hash % stepSizes.size)
    // logging.warn(this, s"Home index is ${homeInvoker}")

    val (chosen, finalFqn: FullyQualifiedEntityName, memSlots: Int, cpuCores: Int) = if (invokersToUse.nonEmpty) {
      val (invoker: Option[(InvokerInstanceId, Boolean)], function: FullyQualifiedEntityName, slots: Int, cores: Int) = LachesisScheduler.schedule(
        action.limits.concurrency.maxConcurrent,
        action.fullyQualifiedName(true),
        invokersToUse,
        schedulingState.containerMap,
        schedulingState.invokerSlots,
        schedulingState.invokerCores,
        action.limits.memory.megabytes,
        action.limits.cpu.cores,
        homeInvoker,
        stepSize,
        action.fullyQualifiedName(true),
        msg.activationId)
      invoker.foreach {
        case (_, true) =>
          val metric =
            if (isBlackboxInvocation)
              LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else
              LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }
      (invoker.map(_._1), function, slots, cores)
    } else {
      (None, None, None, None)
    }

    // Create final action limits, action, and message for the invocation we want to run
    val finalLimits = ActionLimits(action.limits.timeout, MemoryLimit(memSlots.MB), CpuLimit(cpuCores), action.limits.network, action.limits.logs, action.limits.concurrency)
    val finalAction = ExecutableWhiskActionMetaData(action.namespace, 
                                                    finalFqn.name, 
                                                    action.exec, 
                                                    action.parameters, 
                                                    finalLimits, 
                                                    action.version, 
                                                    action.publish, 
                                                    action.annotations,
                                                    action.binding
                                                  )
    val updatedMsgContent: Option[JsObject] = msg.content.map {
      case existingJsObject: JsObject =>
        // Create a new JsObject with the existing fields and the new key-value pair
        JsObject(existingJsObject.fields + ("background" -> JsFalse))
      case _ => JsObject("background" -> JsFalse)
    }
    val finalMsg = ActivationMessage(msg.transid, finalFqn, msg.revision, msg.user, msg.activationId, msg.rootControllerIndex, msg.blocking, updatedMsgContent, msg.initArgs, msg.lockedArgs, msg.cause, msg.traceContext)

    // Create new background message to create warm right size container in the background
    val backgroundMsgContent: Option[JsObject] = msg.content.map {
      case existingJsObject: JsObject =>
        JsObject(existingJsObject.fields + ("background" -> JsTrue))
      case _ => JsObject("background" -> JsTrue)
    }
    val backgroundActivationId = activationIdFactory.make()
    val backgroundMsg = ActivationMessage(msg.transid, msg.action, msg.revision, msg.user, backgroundActivationId, msg.rootControllerIndex, msg.blocking, backgroundMsgContent, msg.initArgs, msg.lockedArgs, msg.cause, msg.traceContext)

    chosen
      .map { invoker =>

        // MemoryLimit() and CpuLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        val memoryLimit = finalAction.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
        val cpuLimit = finalAction.limits.cpu
        val cpuLimitInfo = if (cpuLimit == CpuLimit()) { "std" } else { "non-std" }
        val timeLimit = finalAction.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        logging.info(
          this,
          s"scheduled activation ${finalMsg.activationId}, action '${finalMsg.action.asString}' ($actionType), ns '${finalMsg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), cpu limit ${cpuLimit.cores} cores (${cpuLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")        

        // Spin up and initialize a warm, right-size container in the background for the action beacuse we scheduled the invocation to a larger warm container
        if (finalAction.limits.cpu.cores > action.limits.cpu.cores || finalAction.limits.memory.megabytes > action.limits.memory.megabytes) {        
          schedulingState.addOrDecreaseContainerState(invoker.toInt, action.fullyQualifiedName(true))
          val backgroundActivationResult = setupActivation(backgroundMsg, action, invoker, action.limits.cpu.cores)
          sendActivationToInvoker(messageProducer, backgroundMsg, invoker).map(_ => backgroundActivationResult)
        }

        // Update containerMap state
        schedulingState.addOrDecreaseContainerState(invoker.toInt, finalFqn)
        val activationResult = setupActivation(finalMsg, finalAction, invoker, action.limits.cpu.cores)
        sendActivationToInvoker(messageProducer, finalMsg, invoker).map(_ => activationResult)
      }
      .getOrElse {
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"failed to schedule activation ${finalMsg.activationId}, action '${finalMsg.action.asString}' ($actionType), ns '${finalMsg.user.namespace.name.asString}' - invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  override val invokerPool =
    invokerPoolFactory.createInvokerPool(
      actorSystem,
      messagingProvider,
      messageProducer,
      sendActivationToInvoker,
      Some(monitor))

  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {

    entry.backgroundActivation match {
      // Completed invocation was a user invocation, not one created by the scheduler
      case Some(false) =>
        schedulingState.invokerSlots
        .lift(invoker.toInt)
        .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
      schedulingState.invokerCores
        .lift(invoker.toInt)
        .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.cpuLimit.toInt))
      // logging.warn(this, s"Lachesis - releasing slots or cores for '${entry.id}'")
      // Completed invocation was a background activation to create perfect size warm container by the scheduler
      case _ => logging.warn(this, s"Lachesis - not releasing slots or cores, background activation '${entry.id}'")
    }
    schedulingState.increaseContainerState(invoker.toInt, entry.fullyQualifiedEntityName)
  }
}

object LachesisScheduler extends LoadBalancerProvider {

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(implicit actorSystem: ActorSystem,
                                                                                  logging: Logging): LoadBalancer = {

    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messagingProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[ResultMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(
              whiskConfig,
              s"${Controller.topicPrefix}health${instance.asString}",
              s"${Controller.topicPrefix}health",
              maxPeek = 128),
            monitor))
      }

    }
    new LachesisScheduler(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def requiredProperties: Map[String, String] = kafkaHosts

  /** Generates a hash based on the string representation of namespace and action */
  def generateHash(namespace: EntityName, action: FullyQualifiedEntityName): Int = {
    (namespace.asString.hashCode() ^ action.asString.hashCode()).abs
  }

  /** Euclidean algorithm to determine the greatest-common-divisor */
  @tailrec
  def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)

  /** Returns pairwise coprime numbers until x. Result is memoized. */
  def pairwiseCoprimeNumbersUntil(x: Int): IndexedSeq[Int] =
    (1 to x).foldLeft(IndexedSeq.empty[Int])((primes, cur) => {
      if (gcd(cur, x) == 1 && primes.forall(i => gcd(i, cur) == 1)) {
        primes :+ cur
      } else primes
    })
  
  /** Returns true if fqn can receive requested allocation of memory and cores */
  def getResources(
    invoker: InvokerHealth, 
    dispatchedMem: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    dispatchedCpu: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    fqn: FullyQualifiedEntityName,
    maxConcurrent: Int,
    slots: Int,
    cores: Int)(implicit logging: Logging): Boolean = {
    var returnVal = false
    this.synchronized {
      if (invoker.status.isUsable) {
        if (dispatchedMem(invoker.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) {
          if (dispatchedCpu(invoker.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, cores)) {
            returnVal = true
          // Couldn't get requested cores, so release memory slots obtained
          } else {
            dispatchedMem(invoker.id.toInt).releaseConcurrent(fqn, maxConcurrent, slots)
          }
        }
      }
    }
    returnVal
  }

  /**
   * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
   * obtained, randomly picks a healthy invoker.
   *
   * @param maxConcurrent concurrency limit supported by this action
   * @param invokers a list of available invokers to search in, including their state
   * @param containerMap a list of maps, one per invoker, holding the number of containers per function and cpu limit
   * @param dispatchedMem semaphores for each invoker to give the slots away from
   * @param dispatchedCpu semaphores for each invoker to give the cores away from
   * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
   * @param cores Number of cores, that need to be acquired (e.g. 4 cores)
   * @param index the index to start from (initially should be the "homeInvoker")
   * @param step stable identifier of the entity to be scheduled
   * @param chosenFqnName the function name that will be used when scheduling the invocation with requested resources
   * @return an invoker to schedule to, the exact function name we will be scheduling, the number of cores, and the amount of memory of the container
   */
  @tailrec
  def schedule(
    maxConcurrent: Int,
    fqn: FullyQualifiedEntityName,
    invokers: IndexedSeq[InvokerHealth],
    containerMap: IndexedSeq[mutable.Map[FullyQualifiedEntityName, Int]],
    dispatchedMem: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    dispatchedCpu: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
    slots: Int,
    cores: Int,
    index: Int,
    step: Int,
    chosenFqn: FullyQualifiedEntityName,
    activationId: ActivationId,
    stepsDone: Int = 0,
    chosenIndex: Int = -1,
    chosenDifferenceCpu: Int = 100,
    chosenDifferenceMem: Int = 10000,
    finalRule: Int = -1)(implicit logging: Logging, transId: TransactionId): (Option[(InvokerInstanceId, Boolean)], FullyQualifiedEntityName, Int, Int) = {
    val numInvokers = invokers.size

    if (numInvokers > 0) {
      if (stepsDone == numInvokers || finalRule == 1) {
        // Found an Invoker that fit into one of the three rules
        if (chosenIndex != -1) {
          // logging.warn(this, s"The chosen index was not empty, rather it was ${chosenIndex}")
          val invoker = invokers(chosenIndex)
          val finalCores = chosenFqn.name.toString.split("_")(1).toInt
          val finalSlots = chosenFqn.name.toString.split("_")(2).toInt
          logging.warn(this, s"Lachesis launching: activation_id ${activationId} in rule ${finalRule} index ${chosenIndex} original name ${fqn.name} final name ${chosenFqn.name} rps 5000 over 50 threads cpp take 2")
          (Some(invoker.id, false), chosenFqn, finalSlots, finalCores)
        // Case #6: None of the invokers have any warm containers or space for new containers, choosen one at random
        } else {
          logging.warn(this, s"Lachesis launching: activation_id ${activationId} in case 5 it was rejected rps 5000 over 50 threads cpp take 2")
          // (None, chosenFqn, slots, cores)
          val healthyInvokers = invokers.filter(_.status.isUsable)
          if (healthyInvokers.nonEmpty) {
            // Choose a healthy invoker randomly
            val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size)).id
            dispatchedMem(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, slots)
            dispatchedCpu(random.toInt).forceAcquireConcurrent(fqn, maxConcurrent, cores)
            // logging.warn(this, s"system is overloaded. Chose invoker${random.toInt} by random assignment.")
            (Some(random, true), fqn, slots, cores)
          } else {
            (None, fqn, slots, cores)
          }
        }
      } else {
        // Current state
        var bestIndex = chosenIndex
        var bestDiffCpu = chosenDifferenceCpu
        var bestDiffMem = chosenDifferenceMem
        var bestFqn = chosenFqn
        var currentRule = finalRule

        // Get the invoker and invoker map for current recursive call we are in
        val invoker = invokers(index)
        val invokerMap = containerMap(index)

        // Containers exist on Invoker
        if (!invokerMap.isEmpty) {
          // Search for best container on the Invoker
          for ((mapFqn, count) <- invokerMap) {
            if (currentRule !=1) {
              val mapFqnName: String = mapFqn.name.toString.split("_")(0)
              val mapFqnCores: Int = mapFqn.name.toString.split("_")(1).toInt
              val mapFqnMem: Int = mapFqn.name.toString.split("_")(2).toInt
              val currFqnName: String = fqn.name.toString.split("_")(0)

              if (mapFqnName == currFqnName) {
                val diffCpu = mapFqnCores - cores
                val diffMem = mapFqnMem - slots

                // Case #1: Found warm container that is perfect size and invoker has space
                if (diffCpu == 0 && diffMem == 0 && count > 0) {
                  // Check if we can reserve resources on this invoker for the current container we are considering
                  if (getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, slots, cores)) {
                    // Check if we reserved resources on this invoker or a previous invoker, if so release those resources
                    if (bestIndex != -1) {
                      dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
                      dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
                    }
                    bestIndex = index
                    bestDiffCpu = 0
                    bestDiffMem = 0
                    bestFqn = mapFqn
                    currentRule = 1
                  }
                }

                // Case #2: Found a warm container that is closest in memory and has enough cores compared to the requested size
                else if (diffCpu >= 0 && diffMem >= 0 && diffMem < bestDiffMem && count > 0) {
                  // Check if we can reserve resources on this invoker for the current container we are considering
                  if (getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, mapFqnMem, cores)) {
                    // Check if we reserved resources on this invoker or a previous invoker, if so release those resources
                    if (bestIndex != -1) {
                      dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
                      dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
                    } 
                    bestIndex = index
                    bestDiffCpu = diffCpu
                    bestDiffMem = diffMem
                    bestFqn = mapFqn
                    currentRule = 2
                  }
                }
                
                // Case #3: Found a container equal in memory to the closest we've seen, so see if it's closer in cores
                else if (diffMem >= 0 && diffMem == bestDiffMem && count > 0 && diffCpu >= 0 && diffCpu < bestDiffCpu) {
                  // Check if we can reserve resources on this invoker for the current container we are considering
                  if (getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, mapFqnMem, cores)) {
                    // Check if we reserved resources on this invoker or a previous invoker, if so release those resources
                    if (bestIndex != -1) {
                      dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
                      dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
                    } 
                    bestIndex = index
                    bestDiffCpu = diffCpu
                    bestDiffMem = diffMem
                    bestFqn = mapFqn
                    currentRule = 3
                  }
                }
              }
            }
          }

          // Case #4: Haven't found an invoker with perfect or larger warm container, so check if current invoker has space for requested allocation
          if (bestIndex == -1 && getResources(invoker, dispatchedMem, dispatchedCpu, fqn, maxConcurrent, slots, cores)) {
            bestIndex = index
            currentRule = 4
          }
        } else {
          // Case #5: Haven't found an invoker with a perfect or larger warm container, so check if current invoker has space for requested allocation
          if (bestIndex == -1 && getResources(invoker, dispatchedMem, dispatchedCpu, fqn, maxConcurrent, slots, cores)) {
            bestIndex = index
            currentRule = 5
          }
        }
        val newIndex = (index + step) % numInvokers
        schedule(maxConcurrent, fqn, invokers, containerMap, dispatchedMem, dispatchedCpu, slots, cores, newIndex, step, bestFqn, activationId, stepsDone + 1, bestIndex, bestDiffCpu, bestDiffMem, currentRule)
      }  
    } else {
      (None, fqn, slots, cores)
    }
  }
}

/**
 * Holds the state necessary for scheduling of actions.
 *
 * @param _invokers all of the known invokers in the system
 * @param _containerMap: list of maps, one per invoker, holding state of number of containers per function and resource limit
 *                       key: string (e.g., linpack_32), value: int representing number of containers
 * @param _managedInvokers all invokers for managed runtimes
 * @param _blackboxInvokers all invokers for blackbox runtimes
 * @param _managedStepSizes the step-sizes possible for the current managed invoker count
 * @param _blackboxStepSizes the step-sizes possible for the current blackbox invoker count
 * @param _invokerSlots state of accessible slots of each invoker
 * @param _invokerCores state of accessible cores of each invoker
 */
case class LachesisSchedulerState(
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _containerMap: IndexedSeq[mutable.Map[FullyQualifiedEntityName, Int]] = IndexedSeq.empty[mutable.Map[FullyQualifiedEntityName, Int]],
  private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
  private var _managedStepSizes: Seq[Int] = LachesisScheduler.pairwiseCoprimeNumbersUntil(0),
  private var _blackboxStepSizes: Seq[Int] = LachesisScheduler.pairwiseCoprimeNumbersUntil(0),
  protected[loadBalancer] var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
    IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
  protected[loadBalancer] var _invokerCores: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
    IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
  private var _clusterSize: Int = 1)(
  lbConfig: ShardingContainerPoolBalancerConfig =
    loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging) {

  // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
  // 1.0 and 2.0.
  // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
  // means, that there is no differentiation between managed and blackbox invokers.
  // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
  // specified in config and adapted to the managed fraction.
  private val managedFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.managedFraction))
  private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, lbConfig.blackboxFraction))
  logging.info(this, s"managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
    TransactionId.loadbalancer)

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def containerMap: IndexedSeq[mutable.Map[FullyQualifiedEntityName, Int]] = _containerMap
  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
  def managedStepSizes: Seq[Int] = _managedStepSizes
  def blackboxStepSizes: Seq[Int] = _blackboxStepSizes
  def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots
  def invokerCores: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerCores
  def clusterSize: Int = _clusterSize

  /**
   * @param memory
   * @return calculated invoker slot
   */
  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    val invokerShardMemorySize = memory / _clusterSize
    val newTreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY) {
      logging.error(
        this,
        s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
          + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
          + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
        TransactionId.loadbalancer)
      MemoryLimit.MIN_MEMORY
    } else {
      invokerShardMemorySize
    }
    newTreshold
  }

  def addOrDecreaseContainerState(invokerId: Int, container: FullyQualifiedEntityName) {
    this.synchronized {
      var map = _containerMap(invokerId)
      if (map.contains(container)) {
        if (map(container) > 0) {
          map(container) -= 1
        }
      } else {
        map += (container -> 0)
      }
      _containerMap = _containerMap.updated(invokerId, map)
    }
  }

  def increaseContainerState(invokerId: Int, container: FullyQualifiedEntityName) {
    this.synchronized {
      var map = _containerMap(invokerId)
      if (map.contains(container)) {
        map(container) += 1
      }
      _containerMap = _containerMap.updated(invokerId, map)
    }
  }
  
  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    // for small N, allow the managed invokers to overlap with blackbox invokers, and
    // further assume that blackbox invokers << managed invokers
    val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

    _invokers = newInvokers
    _managedInvokers = _invokers.take(managed)
    _blackboxInvokers = _invokers.takeRight(blackboxes)

    val logDetail = if (oldSize != newSize) {
      _managedStepSizes = LachesisScheduler.pairwiseCoprimeNumbersUntil(managed)
      _blackboxStepSizes = LachesisScheduler.pairwiseCoprimeNumbersUntil(blackboxes)

      if (oldSize < newSize) {
        // Keeps the existing state..
        val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
        _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
          new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
        }
        _invokerCores = _invokerCores ++ onlyNewInvokers.map { invoker =>
          new NestedSemaphore[FullyQualifiedEntityName](90)
        }
        _containerMap = _containerMap ++ onlyNewInvokers.map { invoker =>
          mutable.Map[FullyQualifiedEntityName, Int]()
        }
        val newInvokerDetails = onlyNewInvokers
          .map(i =>
            s"${i.id.toString}: ${i.status}  | ${getInvokerSlot(i.id.userMemory).toMB.MB} of ${i.id.userMemory.toMB.MB} | 90 cores available for Invoker")
          .mkString(", ")
        s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
      } else {
        s"number of known invokers decreased: new = $newSize, old = $oldSize."
      }
    } else {
      s"no update required - number of known invokers unchanged: $newSize."
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes. $logDetail")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      val oldSize = _clusterSize
      _clusterSize = actualSize
      _invokerSlots = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
      }
      _invokerCores = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](90)
      }
      // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
      val invokerCount = _invokers.size
      val totalInvokerMemory =
        _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory).toMB).MB
      val averageInvokerMemory =
        if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
          (totalInvokerMemory / invokerCount).toMB.MB
        } else {
          0.MB
        }
      val totalInvokerCore = 
        _invokers.foldLeft(0L)((total, invoker) => total + 90)
      val averageInvokerCore = 
        if (totalInvokerCore > 0 && invokerCount > 0) {
          (totalInvokerCore / invokerCount)
        } else {
          0
        }
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. ${invokerCount} invokers with ${averageInvokerMemory} average memory size and ${averageInvokerCore} average number of cores - total invoker memory ${totalInvokerMemory} and total invoker cores ${totalInvokerCore}. ")(
        TransactionId.loadbalancer)
    }
  }
}

// /**
//  * State kept for each activation slot until completion.
//  *
//  * @param id id of the activation
//  * @param namespaceId namespace that invoked the action
//  * @param invokerName invoker the action is scheduled to
//  * @param memoryLimit memory limit of the invoked action
//  * @param timeLimit time limit of the invoked action
//  * @param maxConcurrent concurrency limit of the invoked action
//  * @param fullyQualifiedEntityName fully qualified name of the invoked action
//  * @param timeoutHandler times out completion of this activation, should be canceled on good paths
//  * @param isBlackbox true if the invoked action is a blackbox action, otherwise false (managed action)
//  * @param isBlocking true if the action is invoked in a blocking fashion, i.e. "somebody" waits for the result
//  * @param controllerId id of the controller that this activation comes from
//  */
// case class ActivationEntry(id: ActivationId,
//                            namespaceId: UUID,
//                            invokerName: InvokerInstanceId,
//                            memoryLimit: ByteSize,
//                            cpuLimit: Int,
//                            timeLimit: FiniteDuration,
//                            maxConcurrent: Int,
//                            fullyQualifiedEntityName: FullyQualifiedEntityName,
//                            timeoutHandler: Cancellable,
//                            isBlackbox: Boolean,
//                            isBlocking: Boolean,
//                            controllerId: ControllerInstanceId = ControllerInstanceId("0"),
//                            backgroundActivation: Option[Boolean])

// Inside case 1 logic
// // Obtained a bigger container on a different invoker but found the perfect size on this this invoker
// if (index != bestIndex) {
//   // Check if invoker has space for perfect size
//   if (getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, slots, cores)) {
//     // Only release resources if we obtained a container on previous invoker
//     if (bestIndex != -1) {
//       // dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, slots)
//       dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
//       dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
//     }
//     bestIndex = index
//     bestDiff = 0
//     bestDiffMem = 0
//     bestFqn = mapFqn
//     currentRule = 1
//   }
// }
// // Obtained a perfect size container in this invoker but found a larger one on this invoker previously:
// // 1. Release previous container resources
// // 2. Obtain perfect size container resources -- guaranteed to obtain these resources
// else {
//   // dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, slots)
//   dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
//   dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
//   getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, slots, cores)
//   bestIndex = index
//   bestDiff = 0
//   bestDiffMem = 0
//   bestFqn = mapFqn
//   currentRule = 1
// } 

// Inside case 2 logic
// Case #2: Found a larger warm container that is closest in memory and greater in cores compared to the requested size
// else if (diffCores >= 0 && diffMem >= 0 && diffMem < bestDiffMem && count > 0) {
//   // Obtained a bigger container on a different invoker but found a better size on this invoker
//   if (index != bestIndex) {
//     if (getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, mapFqnMem, cores)) {
//       // Only release resources if we obtained a container on previous invoker
//       if (bestIndex != -1) {
//         // dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, slots)
//         dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
//         dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
//       }
//       bestIndex = index
//       bestDiff = diffCpu
//       bestDiffMem = diffMem
//       bestFqn = mapFqn
//       currentRule = 2
//     }
//   }
//   // Obtained a bigger container in this invoker previously but found a better one on this invoker now:
//   // 1. Release previous container resources
//   // 2. Obtain better container resources -- guaranteed to obtain these resources
//   else {
//     // dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, slots)
//     dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
//     dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
//     getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, mapFqnMem, cores)
//     bestIndex = index
//     bestDiff = diffCpu
//     bestDiffMem = diffMem
//     bestFqn = mapFqn
//     currentRule = 2
//   }
// }

// Inside Case 3 Logic
// if (index != bestIndex) {
//   if (getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, mapFqnMem, cores)) {
//     // Only release resources if we obtained a container on previous invoker
//     if (bestIndex != -1) {
//       dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
//       dispatchedCPU(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
//     }
//     bestIndex = index
//     bestDiff = diffCpu
//     bestDiffMem = diffMem
//     bestFqn = mapFqn
//     currentRule = 3
//   }
// }
// // Obtained a bigger container in this invoker previously but found a better one on this invoker now:
// // 1. Release previous container resources
// // 2. Obtain better container resources -- guaranteed to obtain these resources
// else {
//   dispatchedMem(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, bestFqn.name.toString.split("_")(2).toInt)
//   dispatchedCpu(invokers(bestIndex).id.toInt).releaseConcurrent(bestFqn, maxConcurrent, cores)
//   getResources(invoker, dispatchedMem, dispatchedCpu, mapFqn, maxConcurrent, mapFqnMem, cores)
//   bestIndex = index
//   bestDiff = diffCpu
//   bestDiffMem = diffMem
//   bestFqn = mapFqn
//   currentRule = 3
// }