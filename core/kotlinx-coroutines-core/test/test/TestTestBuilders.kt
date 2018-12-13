package kotlinx.coroutines.test

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.*
import org.junit.Assert.*
import kotlin.coroutines.ContinuationInterceptor

class TestTestBuilders {

    @Test
    fun scopeRunBlocking_passesDispatcher() {
        val scope = TestCoroutineScope()
        scope.runBlocking {
            assertSame(scope.coroutineContext[ContinuationInterceptor], coroutineContext[ContinuationInterceptor])
        }
    }

    @Test
    fun dispatcherRunBlocking_passesDispatcher() {
        val dispatcher = TestCoroutineDispatcher()
        dispatcher.runBlocking {
            assertSame(dispatcher, coroutineContext[ContinuationInterceptor])
        }
    }

    @Test
    fun scopeRunBlocking_advancesPreviousDelay() {
        val scope = TestCoroutineScope()
        val deferred = scope.async {
            delay(SLOW)
            3
        }

        scope.runBlocking {
            assertRunsFast {
                assertEquals(3, deferred.await())
            }
        }
    }

    @Test
    fun dispatcherRunBlocking_advancesPreviousDelay() {
        val dispatcher = TestCoroutineDispatcher()
        val scope = CoroutineScope(dispatcher)
        val deferred = scope.async {
            delay(SLOW)
            3
        }

        dispatcher.runBlocking {
            assertRunsFast {
                assertEquals(3, deferred.await())
            }
        }
    }

    @Test
    fun scopeRunBlocking_disablesImmedateOnExit() {
        val scope = TestCoroutineScope()
        scope.runBlocking {
            assertRunsFast {
                delay(SLOW)
            }
        }

        val deferred = scope.async {
            delay(SLOW)
            3
        }
        scope.runCurrent()
        assertTrue(deferred.isActive)

        scope.advanceTimeToNextDelayed()
        assertEquals(3, deferred.getCompleted())
    }

    @Test
    fun whenInAsync_runBlocking_nestsProperly() {
        // this is not a supported use case, but it is possible so ensure it works

        val scope = TestCoroutineScope()
        val deferred = scope.async {
            delay(1_000)
            runBlockingTest {
                delay(1_000)
            }
            3
        }

        assertFalse(scope.dispatchImmediately)

        scope.advanceTimeToNextDelayed()
        scope.launch {
            assertRunsFast {
                assertEquals(3, deferred.getCompleted())
            }
        }
        scope.runCurrent() // execute the launch without changing to immediate dispatch (testing internals)
        scope.cleanupTestCoroutines()
    }

    @Test
    fun whenInrunBlocking_asyncTest_nestsProperly() {
        // this is not a supported use case, but it is possible so ensure it works

        val scope = TestCoroutineScope()
        var calls = 0

        val result = scope.runBlocking {
            delay(1_000)
            calls++
            asyncTest {
                val job = launch {
                    delay(1_000)
                    calls++
                }
                assertTrue(job.isActive)
                runUntilIdle()
                assertFalse(job.isActive)
                calls++
            }
            ++calls
        }

        assertEquals(4, result)
    }
}