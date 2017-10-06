package net.corda.node.services.statemachine

import co.paralleluniverse.fibers.Fiber
import co.paralleluniverse.fibers.Fiber.parkAndSerialize
import co.paralleluniverse.fibers.FiberScheduler
import co.paralleluniverse.fibers.Suspendable
import co.paralleluniverse.strands.Strand
import com.google.common.primitives.Primitives
import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.random63BitValue
import net.corda.core.flows.*
import net.corda.core.identity.Party
import net.corda.core.identity.PartyAndCertificate
import net.corda.core.internal.*
import net.corda.core.internal.concurrent.OpenFuture
import net.corda.core.internal.concurrent.openFuture
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.*
import net.corda.node.services.api.FlowAppAuditEvent
import net.corda.node.services.api.FlowPermissionAuditEvent
import net.corda.node.services.api.ServiceHubInternal
import net.corda.node.services.config.FullNodeConfiguration
import net.corda.node.services.statemachine.FlowSessionState.Initiating
import net.corda.node.utilities.CordaPersistence
import net.corda.node.utilities.DatabaseTransaction
import net.corda.node.utilities.DatabaseTransactionManager
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import java.sql.SQLException
import java.util.*
import java.util.concurrent.TimeUnit

class FlowPermissionException(message: String) : FlowException(message)

class FlowStateMachineImpl<R>(override val id: StateMachineRunId,
                              val logic: FlowLogic<R>,
                              scheduler: FiberScheduler,
                              override val flowInitiator: FlowInitiator,
        // Store the Party rather than the full cert path with PartyAndCertificate
                              val ourIdentity: Party) : Fiber<Unit>(id.toString(), scheduler), FlowStateMachine<R> {

    companion object {
        // Used to work around a small limitation in Quasar.
        private val QUASAR_UNBLOCKER = Fiber::class.staticField<Any>("SERIALIZER_BLOCKER").value

        /**
         * Return the current [FlowStateMachineImpl] or null if executing outside of one.
         */
        fun currentStateMachine(): FlowStateMachineImpl<*>? = Strand.currentStrand() as? FlowStateMachineImpl<*>

        /**
         * Provide a mechanism to sleep within a Strand without locking any transactional state
         */
        // TODO: inlined due to an intermittent Quasar error (to be fully investigated)
        @Suppress("NOTHING_TO_INLINE")
        @Suspendable
        inline fun sleep(millis: Long) {
            if (currentStateMachine() != null) {
                val db = DatabaseTransactionManager.dataSource
                DatabaseTransactionManager.current().commit()
                DatabaseTransactionManager.current().close()
                Strand.sleep(millis)
                DatabaseTransactionManager.dataSource = db
                DatabaseTransactionManager.newTransaction()
            } else Strand.sleep(millis)
        }
    }

    // These fields shouldn't be serialised, so they are marked @Transient.
    @Transient override lateinit var serviceHub: ServiceHubInternal
    @Transient override lateinit var ourIdentityAndCert: PartyAndCertificate
    @Transient internal lateinit var database: CordaPersistence
    @Transient internal lateinit var actionOnSuspend: (FlowIORequest) -> Unit
    @Transient internal lateinit var actionOnEnd: (Try<R>, Boolean) -> Unit
    @Transient internal var fromCheckpoint: Boolean = false
    @Transient private var txTrampoline: DatabaseTransaction? = null

    /**
     * Return the logger for this state machine. The logger name incorporates [id] and so including it in the log message
     * is not necessary.
     */
    override val logger: Logger = LoggerFactory.getLogger("net.corda.flow.$id")

    @Transient private var _resultFuture: OpenFuture<R>? = openFuture()
    /** This future will complete when the call method returns. */
    override val resultFuture: CordaFuture<R>
        get() = _resultFuture ?: openFuture<R>().also { _resultFuture = it }

    // This state IS serialised, as we need it to know what the fiber is waiting for.
    internal val openSessions = HashMap<Pair<FlowLogic<*>, Party>, FlowSessionInternal>()
    internal var waitingForResponse: WaitingRequest? = null
    internal var hasSoftLockedStates: Boolean = false
        set(value) {
            if (value) field = value else throw IllegalArgumentException("Can only set to true")
        }

    init {
        logic.stateMachine = this
    }

    @Suspendable
    override fun run() {
        createTransaction()
        logger.debug { "Calling flow: $logic" }
        val startTime = System.nanoTime()
        val result = try {
            val r = logic.call()
            // Only sessions which have done a single send and nothing else will block here
            openSessions.values
                    .filter { it.state is Initiating }
                    .forEach { it.waitForConfirmation() }
            r
        } catch (e: FlowException) {
            recordDuration(startTime, success = false)
            // Check if the FlowException was propagated by looking at where the stack trace originates (see suspendAndExpectReceive).
            val propagated = e.stackTrace[0].className == javaClass.name
            processException(e, propagated)
            logger.warn(if (propagated) "Flow ended due to receiving exception" else "Flow finished with exception", e)
            return
        } catch (t: Throwable) {
            recordDuration(startTime, success = false)
            logger.warn("Terminated by unexpected exception", t)
            processException(t, false)
            return
        }

        recordDuration(startTime)
        // This is to prevent actionOnEnd being called twice if it throws an exception
        actionOnEnd(Try.Success(result), false)
        _resultFuture?.set(result)
        logic.progressTracker?.currentStep = ProgressTracker.DONE
        logger.debug { "Flow finished with result ${result.toString().abbreviate(300)}" }
    }

    private fun createTransaction() {
        // Make sure we have a database transaction
        database.createTransaction()
        logger.trace { "Starting database transaction ${DatabaseTransactionManager.currentOrNull()} on ${Strand.currentStrand()}" }
    }

    private fun processException(exception: Throwable, propagated: Boolean) {
        actionOnEnd(Try.Failure(exception), propagated)
        _resultFuture?.setException(exception)
        logic.progressTracker?.endWithError(exception)
    }

    internal fun commitTransaction() {
        val transaction = DatabaseTransactionManager.current()
        try {
            logger.trace { "Committing database transaction $transaction on ${Strand.currentStrand()}." }
            transaction.commit()
        } catch (e: SQLException) {
            // TODO: we will get here if the database is not available.  Think about how to shutdown and restart cleanly.
            logger.error("Transaction commit failed: ${e.message}", e)
            System.exit(1)
        } finally {
            transaction.close()
        }
    }

    @Suspendable
    override fun initiateFlow(otherParty: Party, sessionFlow: FlowLogic<*>): FlowSession {
        val sessionKey = Pair(sessionFlow, otherParty)
        if (openSessions.containsKey(sessionKey)) {
            throw IllegalStateException(
                    "Attempted to initiateFlow() twice in the same InitiatingFlow $sessionFlow for the same party " +
                            "$otherParty. This isn't supported in this version of Corda. Alternatively you may " +
                            "initiate a new flow by calling initiateFlow() in an " +
                            "@${InitiatingFlow::class.java.simpleName} sub-flow."
            )
        }
        createNewSession(otherParty, sessionFlow)
        val flowSession = FlowSessionImpl(otherParty)
        flowSession.stateMachine = this
        flowSession.sessionFlow = sessionFlow
        return flowSession
    }

    @Suspendable
    override fun getFlowInfo(otherParty: Party, sessionFlow: FlowLogic<*>): FlowInfo {
        val state = getConfirmedSession(otherParty, sessionFlow).state as FlowSessionState.Initiated
        return state.context
    }

    @Suspendable
    override fun <T : Any> sendAndReceive(receiveType: Class<T>,
                                          otherParty: Party,
                                          payload: Any,
                                          sessionFlow: FlowLogic<*>,
                                          retrySend: Boolean): UntrustworthyData<T> {
        requireNonPrimitive(receiveType)
        logger.debug { "sendAndReceive(${receiveType.name}, $otherParty, ${payload.toString().abbreviate(300)}) ..." }
        val session = getConfirmedSessionIfPresent(otherParty, sessionFlow)
        val receivedSessionData: ReceivedSessionMessage<SessionData> = if (session == null) {
            val newSession = initiateSession(otherParty, sessionFlow, payload, waitForConfirmation = true, retryable = retrySend)
            // Only do a receive here as the session init has carried the payload
            receiveInternal(newSession, receiveType)
        } else {
            val sendData = createSessionData(session, payload)
            sendAndReceiveInternal(session, sendData, receiveType)
        }
        logger.debug { "Received ${receivedSessionData.message.payload.toString().abbreviate(300)}" }
        return receivedSessionData.checkPayloadIs(receiveType)
    }

    @Suspendable
    override fun <T : Any> receive(receiveType: Class<T>,
                                   otherParty: Party,
                                   sessionFlow: FlowLogic<*>): UntrustworthyData<T> {
        requireNonPrimitive(receiveType)
        logger.debug { "receive(${receiveType.name}, $otherParty) ..." }
        val session = getConfirmedSession(otherParty, sessionFlow)
        val sessionData = receiveInternal<SessionData>(session, receiveType)
        logger.debug { "Received ${sessionData.message.payload.toString().abbreviate(300)}" }
        return sessionData.checkPayloadIs(receiveType)
    }

    private fun requireNonPrimitive(receiveType: Class<*>) {
        require(!receiveType.isPrimitive) {
            "Use the wrapper type ${Primitives.wrap(receiveType).name} instead of the primitive $receiveType.class"
        }
    }

    @Suspendable
    override fun send(otherParty: Party, payload: Any, sessionFlow: FlowLogic<*>) {
        logger.debug { "send($otherParty, ${payload.toString().abbreviate(300)})" }
        val session = getConfirmedSessionIfPresent(otherParty, sessionFlow)
        if (session == null) {
            // Don't send the payload again if it was already piggy-backed on a session init
            initiateSession(otherParty, sessionFlow, payload, waitForConfirmation = false)
        } else {
            sendInternal(session, createSessionData(session, payload))
        }
    }

    @Suspendable
    override fun waitForLedgerCommit(hash: SecureHash, sessionFlow: FlowLogic<*>): SignedTransaction {
        logger.debug { "waitForLedgerCommit($hash) ..." }
        suspend(WaitForLedgerCommit(hash, sessionFlow.stateMachine as FlowStateMachineImpl<*>))
        val stx = serviceHub.validatedTransactions.getTransaction(hash)
        if (stx != null) {
            logger.debug { "Transaction $hash committed to ledger" }
            return stx
        }

        // If the tx isn't committed then we may have been resumed due to an session ending in an error
        for (session in openSessions.values) {
            for (receivedMessage in session.receivedMessages) {
                if (receivedMessage.message is ErrorSessionEnd) {
                    session.erroredEnd(receivedMessage.message)
                }
            }
        }
        throw IllegalStateException("We were resumed after waiting for $hash but it wasn't found in our local storage")
    }

    // TODO Dummy implementation of access to application specific permission controls and audit logging
    override fun checkFlowPermission(permissionName: String, extraAuditData: Map<String, String>) {
        // This is a hack to allow cash app access list of permitted issuer currency.
        // TODO: replace this with cordapp configuration.
        val config = serviceHub.configuration as? FullNodeConfiguration
        val permissionGranted = config?.extraAdvertisedServiceIds?.contains(permissionName) ?: true
        val checkPermissionEvent = FlowPermissionAuditEvent(
                serviceHub.clock.instant(),
                flowInitiator,
                "Flow Permission Required: $permissionName",
                extraAuditData,
                logic.javaClass,
                id,
                permissionName,
                permissionGranted)
        serviceHub.auditService.recordAuditEvent(checkPermissionEvent)
        if (!permissionGranted) {
            throw FlowPermissionException("User $flowInitiator not permissioned for $permissionName on flow $id")
        }
    }

    // TODO Dummy implementation of access to application specific audit logging
    override fun recordAuditEvent(eventType: String, comment: String, extraAuditData: Map<String, String>) {
        val flowAuditEvent = FlowAppAuditEvent(
                serviceHub.clock.instant(),
                flowInitiator,
                comment,
                extraAuditData,
                logic.javaClass,
                id,
                eventType)
        serviceHub.auditService.recordAuditEvent(flowAuditEvent)
    }

    @Suspendable
    override fun flowStackSnapshot(flowClass: Class<out FlowLogic<*>>): FlowStackSnapshot? {
        return FlowStackSnapshotFactory.instance.getFlowStackSnapshot(flowClass)
    }

    override fun persistFlowStackSnapshot(flowClass: Class<out FlowLogic<*>>) {
        FlowStackSnapshotFactory.instance.persistAsJsonFile(flowClass, serviceHub.configuration.baseDirectory, id)
    }

    /**
     * This method will suspend the state machine and wait for incoming session init response from other party.
     */
    @Suspendable
    private fun FlowSessionInternal.waitForConfirmation() {
        val (peerParty, sessionInitResponse) = receiveInternal<SessionInitResponse>(this, null)
        if (sessionInitResponse is SessionConfirm) {
            state = FlowSessionState.Initiated(
                    peerParty,
                    sessionInitResponse.initiatedSessionId,
                    FlowInfo(sessionInitResponse.flowVersion, sessionInitResponse.appName))
        } else {
            sessionInitResponse as SessionReject
            throw UnexpectedFlowEndException("Party ${state.sendToParty} rejected session request: ${sessionInitResponse.errorMessage}")
        }
    }

    private fun createSessionData(session: FlowSessionInternal, payload: Any): SessionData {
        val sessionState = session.state
        val peerSessionId = when (sessionState) {
            is FlowSessionState.Initiated -> sessionState.peerSessionId
            else -> throw IllegalStateException("We've somehow held onto a non-initiated session: $session")
        }
        return SessionData(peerSessionId, payload)
    }

    @Suspendable
    private fun sendInternal(session: FlowSessionInternal, message: SessionMessage) = suspend(SendOnly(session, message))

    private inline fun <reified M : ExistingSessionMessage> receiveInternal(
            session: FlowSessionInternal,
            userReceiveType: Class<*>?): ReceivedSessionMessage<M> {
        return waitForMessage(ReceiveOnly(session, M::class.java, userReceiveType))
    }

    private inline fun <reified M : ExistingSessionMessage> sendAndReceiveInternal(
            session: FlowSessionInternal,
            message: SessionMessage,
            userReceiveType: Class<*>?): ReceivedSessionMessage<M> {
        return waitForMessage(SendAndReceive(session, message, M::class.java, userReceiveType))
    }

    @Suspendable
    private fun getConfirmedSessionIfPresent(otherParty: Party, sessionFlow: FlowLogic<*>): FlowSessionInternal? {
        val session = openSessions[Pair(sessionFlow, otherParty)] ?: return null
        return when (session.state) {
            is FlowSessionState.Uninitiated -> null
            is FlowSessionState.Initiating -> {
                session.waitForConfirmation()
                session
            }
            is FlowSessionState.Initiated -> session
        }
    }

    @Suspendable
    private fun getConfirmedSession(otherParty: Party, sessionFlow: FlowLogic<*>): FlowSessionInternal {
        return getConfirmedSessionIfPresent(otherParty, sessionFlow) ?:
                initiateSession(otherParty, sessionFlow, null, waitForConfirmation = true)
    }

    private fun createNewSession(
            otherParty: Party,
            sessionFlow: FlowLogic<*>
    ) {
        logger.trace { "Creating a new session with $otherParty" }
        val session = FlowSessionInternal(sessionFlow, random63BitValue(), null, FlowSessionState.Uninitiated(otherParty))
        openSessions[Pair(sessionFlow, otherParty)] = session
    }

    @Suspendable
    private fun initiateSession(
            otherParty: Party,
            sessionFlow: FlowLogic<*>,
            firstPayload: Any?,
            waitForConfirmation: Boolean,
            retryable: Boolean = false
    ): FlowSessionInternal {
        val session = openSessions[Pair(sessionFlow, otherParty)]
        if (session == null) {
            throw IllegalStateException("Expected an Uninitiated session for $otherParty")
        }
        val state = session.state
        if (state !is FlowSessionState.Uninitiated) {
            throw IllegalStateException("Tried to initiate a session $session, but it's already initiating/initiated")
        }
        logger.trace { "Initiating a new session with ${state.otherParty}" }
        session.state = FlowSessionState.Initiating(state.otherParty)
        session.retryable = retryable
        val (version, initiatingFlowClass) = session.flow.javaClass.flowVersionAndInitiatingClass
        val sessionInit = SessionInit(session.ourSessionId, initiatingFlowClass.name, version, session.flow.javaClass.appName, firstPayload)
        sendInternal(session, sessionInit)
        if (waitForConfirmation) {
            session.waitForConfirmation()
        }
        return session
    }

    @Suspendable
    private fun <M : ExistingSessionMessage> waitForMessage(receiveRequest: ReceiveRequest<M>): ReceivedSessionMessage<M> {
        return receiveRequest.suspendAndExpectReceive().confirmReceiveType(receiveRequest)
    }

    @Suspendable
    private fun ReceiveRequest<*>.suspendAndExpectReceive(): ReceivedSessionMessage<*> {
        fun pollForMessage() = session.receivedMessages.poll()

        val polledMessage = pollForMessage()
        return if (polledMessage != null) {
            if (this is SendAndReceive) {
                // Since we've already received the message, we downgrade to a send only to get the payload out and not
                // inadvertently block
                suspend(SendOnly(session, message))
            }
            polledMessage
        } else {
            // Suspend while we wait for a receive
            suspend(this)
            pollForMessage() ?:
                    throw IllegalStateException("Was expecting a ${receiveType.simpleName} but instead got nothing for $this")
        }
    }

    private fun <M : ExistingSessionMessage> ReceivedSessionMessage<*>.confirmReceiveType(
            receiveRequest: ReceiveRequest<M>): ReceivedSessionMessage<M> {
        val session = receiveRequest.session
        val receiveType = receiveRequest.receiveType
        if (receiveType.isInstance(message)) {
            return uncheckedCast(this)
        } else if (message is SessionEnd) {
            openSessions.values.remove(session)
            if (message is ErrorSessionEnd) {
                session.erroredEnd(message)
            } else {
                val expectedType = receiveRequest.userReceiveType?.name ?: receiveType.simpleName
                throw UnexpectedFlowEndException("Counterparty flow on ${session.state.sendToParty} has completed without " +
                        "sending a $expectedType")
            }
        } else {
            throw IllegalStateException("Was expecting a ${receiveType.simpleName} but instead got $message for $receiveRequest")
        }
    }

    private fun FlowSessionInternal.erroredEnd(end: ErrorSessionEnd): Nothing {
        if (end.errorResponse != null) {
            @Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")
            (end.errorResponse as java.lang.Throwable).fillInStackTrace()
            throw end.errorResponse
        } else {
            throw UnexpectedFlowEndException("Counterparty flow on ${state.sendToParty} had an internal error and has terminated")
        }
    }

    @Suspendable
    private fun suspend(ioRequest: FlowIORequest) {
        // We have to pass the thread local database transaction across via a transient field as the fiber park
        // swaps them out.
        txTrampoline = DatabaseTransactionManager.setThreadLocalTx(null)
        if (ioRequest is WaitingRequest)
            waitingForResponse = ioRequest

        var exceptionDuringSuspend: Throwable? = null
        parkAndSerialize { _, _ ->
            logger.trace { "Suspended on $ioRequest" }
            // restore the Tx onto the ThreadLocal so that we can commit the ensuing checkpoint to the DB
            try {
                DatabaseTransactionManager.setThreadLocalTx(txTrampoline)
                txTrampoline = null
                actionOnSuspend(ioRequest)
            } catch (t: Throwable) {
                // Quasar does not terminate the fiber properly if an exception occurs during a suspend. We have to
                // resume the fiber just so that we can throw it when it's running.
                exceptionDuringSuspend = t
                logger.trace("Resuming so fiber can it terminate with the exception thrown during suspend process", t)
                resume(scheduler)
            }
        }

        createTransaction()
        // TODO Now that we're throwing outside of the suspend the FlowLogic can catch it. We need Quasar to terminate
        // the fiber when exceptions occur inside a suspend.
        exceptionDuringSuspend?.let { throw it }
        logger.trace { "Resumed from $ioRequest" }
    }

    internal fun resume(scheduler: FiberScheduler) {
        try {
            if (fromCheckpoint) {
                logger.info("Resumed from checkpoint")
                fromCheckpoint = false
                Fiber.unparkDeserialized(this, scheduler)
            } else if (state == State.NEW) {
                logger.trace("Started")
                start()
            } else {
                Fiber.unpark(this, QUASAR_UNBLOCKER)
            }
        } catch (t: Throwable) {
            logger.error("Error during resume", t)
        }
    }

    /**
     * Records the duration of this flow – from call() to completion or failure.
     * Note that the duration will include the time the flow spent being parked, and not just the total
     * execution time.
     */
    private fun recordDuration(startTime: Long, success: Boolean = true) {
        val timerName = "FlowDuration.${if (success) "Success" else "Failure"}.${logic.javaClass.name}"
        val timer = serviceHub.monitoringService.metrics.timer(timerName)
        // Start time gets serialized along with the fiber when it suspends
        val duration = System.nanoTime() - startTime
        timer.update(duration, TimeUnit.NANOSECONDS)
    }
}

val Class<out FlowLogic<*>>.flowVersionAndInitiatingClass: Pair<Int, Class<out FlowLogic<*>>>
    get() {
        var current: Class<*> = this
        var found: Pair<Int, Class<out FlowLogic<*>>>? = null
        while (true) {
            val annotation = current.getDeclaredAnnotation(InitiatingFlow::class.java)
            if (annotation != null) {
                if (found != null) throw IllegalArgumentException("${InitiatingFlow::class.java.name} can only be annotated once")
                require(annotation.version > 0) { "Flow versions have to be greater or equal to 1" }
                found = annotation.version to uncheckedCast(current)
            }
            current = current.superclass
                    ?: return found
                    ?: throw IllegalArgumentException("$name, as a flow that initiates other flows, must be annotated with " +
                    "${InitiatingFlow::class.java.name}. See https://docs.corda.net/api-flows.html#flowlogic-annotations.")
        }
    }

val Class<out FlowLogic<*>>.appName: String
    get() {
        val jarFile = Paths.get(protectionDomain.codeSource.location.toURI())
        return if (jarFile.isRegularFile() && jarFile.toString().endsWith(".jar")) {
            jarFile.fileName.toString().removeSuffix(".jar")
        } else {
            "<unknown>"
        }
    }
