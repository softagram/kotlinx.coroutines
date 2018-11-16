package channels_new

import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.suspendAtomicCancellableCoroutine
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.Continuation
import kotlin.math.min

suspend inline fun <R> select(crossinline builder: SelectBuilder<R>.() -> Unit): R {
    val select = SelectInstance<R>()
    builder(select)
    return select.select()
}

suspend inline fun <R> selectUnbiased(crossinline builder: SelectBuilder<R>.() -> Unit): R {
    val select = SelectInstance<R>()
    builder(select)
    select.shuffleAlternatives()
    return select.select()
}

interface SelectBuilder<RESULT> {
    operator fun <FUNC_RESULT> Param0RegInfo<FUNC_RESULT>.invoke(block: (FUNC_RESULT) -> RESULT)

    operator fun <PARAM, FUNC_RESULT> Param1RegInfo<FUNC_RESULT>.invoke(param: PARAM, block: (FUNC_RESULT) -> RESULT)
    operator fun <FUNC_RESULT> Param1RegInfo<FUNC_RESULT>.invoke(block: (FUNC_RESULT) -> RESULT) = invoke(null, block)
}

// channel, selectInstance, element -> is selected by this alternative
typealias RegFunc = Function3<RendezvousChannel<*>, SelectInstance<*>, Any?, RegResult?>
class Param0RegInfo<FUNC_RESULT>(
        @JvmField val channel: Any,
        @JvmField val regFunc: RegFunc,
        @JvmField val actFunc: ActFunc<FUNC_RESULT>
)
class Param1RegInfo<FUNC_RESULT>(
        @JvmField val channel: Any,
        @JvmField val regFunc: RegFunc,
        @JvmField val actFunc: ActFunc<FUNC_RESULT>
)
class RegResult(@JvmField val cleanable: Cleanable?, @JvmField val index: Int)
// continuation, result (usually a received element), block
typealias ActFunc<FUNC_RESULT> = Function2<Any?, Function1<FUNC_RESULT, Any?>, Any?>
class SelectInstance<RESULT> : SelectBuilder<RESULT> {
    private val id = selectInstanceIdGenerator.incrementAndGet()
    private @Volatile var waitingFor: SelectInstance<*>? = null
    private val alternatives = ArrayList<Any?>()
    lateinit var cont: Continuation<in Any>
    private val _state = atomic<Any?>(STATE_REG)

    fun setState(state: Any) { _state.value = state }

    override fun <FUNC_RESULT> Param0RegInfo<FUNC_RESULT>.invoke(block: (FUNC_RESULT) -> RESULT) {
        addAlternative(channel, null, regFunc, actFunc, block)
    }
    override fun <PARAM, FUNC_RESULT> Param1RegInfo<FUNC_RESULT>.invoke(param: PARAM, block: (FUNC_RESULT) -> RESULT) {
        addAlternative(channel, param, regFunc, actFunc, block)
    }
    private fun <FUNC_RESULT> addAlternative(channel: Any, param: Any?, regFunc: Any, actFunc: ActFunc<FUNC_RESULT>, block: (FUNC_RESULT) -> RESULT) {
        alternatives.add(channel)
        alternatives.add(param)
        alternatives.add(regFunc)
        alternatives.add(actFunc)
        alternatives.add(block)
        alternatives.add(null)
        alternatives.add(null)
    }
    /**
     * Shuffles alternatives for [selectUnbiased].
     */
    fun shuffleAlternatives() {
        // This code is based on `Collections#shuffle`,
        // just adapted to our purposes only.
        val size = alternatives.size / ALTERNATIVE_SIZE
        for (i in size - 1 downTo 1) {
            val j = ThreadLocalRandom.current().nextInt(i + 1)
            for (offset in 0 until ALTERNATIVE_SIZE) {
                Collections.swap(alternatives, i * ALTERNATIVE_SIZE + offset, j * ALTERNATIVE_SIZE + offset)
            }
        }
    }
    /**
     * Performs `select` in 3-phase way. At first it selects an alternative atomically
     * (suspending if needed), then it unregisters from unselected channels,
     * and invokes the specified for the selected alternative action at last.
     */
    suspend fun select(): RESULT {
        val result = selectAlternative()
        cleanNonSelectedAlternatives()
        return invokeSelectedAlternativeAction(result)
    }
    /**
     * After this function is invoked it is guaranteed that an alternative is selected
     * and the corresponding channel is stored into `_state` field.
     */
    private suspend fun selectAlternative(): Any? {
        for (i in 0 until alternatives.size step ALTERNATIVE_SIZE) {
            val channel = alternatives[i]!!
            val param = alternatives[i + 1]
            val regFunc = alternatives[i + 2]
            regFunc as RegFunc
            channel as RendezvousChannel<*> // todo FIX TYPES
            val regResult = regFunc(channel, this, param)
            if (regResult == null) { // rendezvous
                val result = this._state.value
                this._state.value = channel
                return result
            } else if (regResult == REG_RESULT_CONFIRMED) {
                break
            } else {
                alternatives[i + 6] = regResult.index
                alternatives[i + 5] = regResult.cleanable
            }
        }
        return suspendAtomicCancellableCoroutine<Any> { cont ->
            this.cont = cont
            this._state.value = STATE_WAITING
        }
    }


    fun trySelect(channel: Any, element: Any, selectFrom: SelectInstance<*>?): Int {
        var state = _state.value
        while (state == STATE_REG) {
            if (selectFrom != null) {
                selectFrom.waitingFor = this
                if (shouldConfirm(selectFrom, selectFrom, selectFrom.id)) {
                    selectFrom.waitingFor = null
                    return TRY_SELECT_CONFIRM
                }
            }
            state = _state.value
        }
        selectFrom?.waitingFor = null

        if (state != STATE_WAITING) { return TRY_SELECT_FAIL }
        if (!_state.compareAndSet(STATE_WAITING, channel)) { return TRY_SELECT_FAIL }
        val cont = this.cont as CancellableContinuation<in Any>
        return if (cont.tryResumeCont(element)) { TRY_SELECT_SUCCESS } else TRY_SELECT_FAIL
    }

    private fun shouldConfirm(start: SelectInstance<*>, cur: SelectInstance<*>, min: Long): Boolean {
        val next = cur.waitingFor ?: return false
        val min = min(min, next.id)
        if (next == start) return min == start.id
        return shouldConfirm(start, next, min)
    }




    /**
     * This function removes this `SelectInstance` from the
     * waiting queues of other alternatives.
     */
    private fun cleanNonSelectedAlternatives() {
        for (i in 0 until alternatives.size step ALTERNATIVE_SIZE) {
            val cleanable = alternatives[i + 5]
            val index = alternatives[i + 6]
            // `cleanable` can be null in case this alternative has not been processed.
            // This means that the next alternatives has not been processed as well.
            if (cleanable === null) break
            cleanable as Cleanable
            cleanable.clean(index as Int)
        }
    }
    /**
     * Gets the act function and the block for the selected alternative and invoke it
     * with the specified result.
     */
    private fun invokeSelectedAlternativeAction(result: Any?): RESULT {
        val i = selectedAlternativeIndex()
        val actFunc = alternatives[i + 3] as ActFunc<in Any>
        val block = alternatives[i + 4] as (Any?) -> RESULT
        return actFunc(result, block) as RESULT
    }
    /**
     * Return an index of the selected alternative in `alternatives` array.
     */
    private fun selectedAlternativeIndex(): Int {
        val channel = _state.value!!
        for (i in 0 until alternatives.size step ALTERNATIVE_SIZE) {
            if (alternatives[i] === channel) return i
        }
        error("Channel $channel is not found")
    }
    companion object {
        @JvmStatic private val selectInstanceIdGenerator = AtomicLong()
        // Number of items to be stored for each alternative in `alternatives` array.
        const val ALTERNATIVE_SIZE = 7

        const val TRY_SELECT_SUCCESS = 0
        const val TRY_SELECT_FAIL = 1
        const val TRY_SELECT_CONFIRM = 2

        val REG_RESULT_CONFIRMED = RegResult(null, 0)

        val STATE_REG = Any()
        val STATE_WAITING = Any()
        val STATE_DONE = Any()
    }
}