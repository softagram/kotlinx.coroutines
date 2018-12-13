package kotlinx.coroutines.test

import kotlinx.coroutines.CoroutineExceptionHandler
import java.util.*
import kotlin.coroutines.CoroutineContext

/**
 * Access uncaught coroutines exceptions captured during test execution.
 *
 * Note, tests executed via [runBlockingTest] or [TestCoroutineScope.runBlocking] will not trigger uncaught exception
 * handling and should use [Deferred.await] or [Job.getCancellationException] to test exceptions.
 */
interface ExceptionCaptor {
    /**
     * List of uncaught coroutine exceptions.
     *
     * Tests must process these exceptions prior to [cleanupTestCoroutines] either by clearing this list with
     * [exceptions.clear] or calling [rethrowUncaughtCoroutineException].
     */
    val exceptions: MutableList<Throwable>

    /**
     * Rethrow the first uncaught coroutine exception immediately.
     *
     * This allows tests to use their preferred exception testing techniques.
     *
     * If a test generates uncaught exceptions, it must call this method, or clear [exceptions] prior to calling
     * [cleanupTestCoroutines].
     */
    fun rethrowUncaughtCoroutineException(): Nothing

    /**
     * Call after the test completes.
     *
     * @throws UnhandledExceptionsError if any exceptions have not been handled via [rethrowUncaughtCoroutineException]
     * or ignored via [exceptions.clear]
     */
    fun cleanupTestCoroutines()
}

/**
 * Thrown when a test completes with uncaught exceptions that have not been handled.
 *
 * @param message descriptive message
 * @param cause the first uncaught exception
 */
class UnhandledExceptionsError(message: String, cause: Throwable): AssertionError(message, cause)

/**
 * An exception handler that can be used to capture uncaught exceptions in tests.
 */
class TestCoroutineExceptionHandler: ExceptionCaptor, CoroutineExceptionHandler {
    override fun handleException(context: CoroutineContext, exception: Throwable) {
        exceptions += exception
    }

    override val key = CoroutineExceptionHandler

    override val exceptions = LinkedList<Throwable>()

    override fun rethrowUncaughtCoroutineException(): Nothing {
        if(!exceptions.isEmpty()) {
            throw exceptions.removeAt(0)
        }
        throw AssertionError("No exceptions were caught to rethrow")
    }

    override fun cleanupTestCoroutines() {
        val exception = exceptions.firstOrNull() ?: return
        throw UnhandledExceptionsError("Unhandled exceptions were not processed by test. " +
                "Call rethrowUncaughtCoroutineException() to handle uncaught exceptions.", exception)
    }
}
