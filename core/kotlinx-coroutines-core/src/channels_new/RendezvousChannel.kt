package channels_new

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.suspendAtomicCancellableCoroutine
import kotlin.coroutines.resume

@ExperimentalUnsignedTypes
class RendezvousChannel<E> : Channel<E> {
    // Waiting queue node
    internal class Node(@JvmField val id: Long, prev: Node?) : Cleanable {
        // This array contains the data of this segment. In order not to have
        // redundant cache misses, both values to be sent and continuations
        // are stored in the same array at indexes `2i` and `2i+1` respectively.
        private val _data = kotlin.arrayOfNulls<Any?>(segmentSize * 2)
        // Pointer to the next node in the waiting queue, maintained similar to
        // MS queue algorithm. This pointer can be either null, REMOVED_TAIL, `Node`, or `RemovedNode`
        // depending on the fact if it is logically removed after the cleaning algorithm part.
        private val _next = atomic<Node?>(null)
        // Counter of cleaned elements. The node should be removed from the waiting queue
        // (in case it is not a head) when the counter exceeds `segmentSize`.
        private val _cleaned = atomic(0)
        // Lazy updated link to the previous node, which is used for cleaning
        private val _prev = atomic(prev)
        val removed get() = _cleaned.value == segmentSize

        override fun clean(index: Int) {
            // Clean the specified node item and
            // check if all node items are cleaned.
            val cont = getCont(index)
            if (cont == TAKEN_CONTINUATION) return
            if (!casCont(index, cont, TAKEN_CONTINUATION)) return
            putElement(index, BROKEN)
            if (_cleaned.incrementAndGet() < segmentSize) return
            // Removing this node
            remove()
        }
        /**
         * Removes this node from the waiting queue and clean all references to it.
         */
        fun remove() {
            var next = next() ?: return // tail can't be removed
            // Find the first non-removed node (tail is always non-removed)
            while (next.removed) {
                next = next.next() ?: break
            }
            // Find the first non-removed prev and remove the node
            var prev = _prev.value
            while (true) {
                if (prev == null) {
                    next.movePrevToLeft(null)
                    return
                }
                if (prev.removed) {
                    prev = prev._prev.value
                    continue
                }
                next.movePrevToLeft(prev)
                prev.moveNextToRight(next)
                if (next.removed || !prev.removed) return
                prev = prev._prev.value
            }
        }
        private fun moveNextToRight(next: Node) {
            while (true) {
                val curNext = _next.value!!
                if (next.id <= curNext.id) return
                if (_next.compareAndSet(curNext, next)) return
            }
        }
        private fun movePrevToLeft(prev: Node?) {
            while (true) {
                val curPrev = _prev.value ?: return
                if (prev != null && curPrev.id <= prev.id) return
                if (_prev.compareAndSet(curPrev, prev)) return
            }
        }
        fun next() = _next.value
        fun casNext(old: Node?, new: Node?) = _next.compareAndSet(old, new)
        fun putPrev(node: Node?) {
            _prev.lazySet(node)
        }
        inline fun putElementVolatile(index: Int, element: Any) {
            UNSAFE.putObjectVolatile(_data, byteOffset(index * 2), element)
        }
        inline fun putElement(index: Int, element: Any) {
            UNSAFE.putOrderedObject(_data, byteOffset(index * 2), element)
        }
        inline fun getElementVolatile(index: Int): Any? {
            return UNSAFE.getObjectVolatile(_data, byteOffset(index * 2))
        }
        inline fun getElement(index: Int): Any? {
            return UNSAFE.getObject(_data, byteOffset(index * 2))
        }
        inline fun casElement(index: Int, expect: Any?, update: Any): Boolean {
            return UNSAFE.compareAndSwapObject(_data, byteOffset(index * 2), expect, update)
        }
        inline fun putContVolatile(index: Int, element: Any) {
            UNSAFE.putObjectVolatile(_data, byteOffset(index * 2 + 1), element)
        }
        inline fun putCont(index: Int, element: Any?) {
            UNSAFE.putOrderedObject(_data, byteOffset(index * 2 + 1), element)
        }
        inline fun getContVolatile(index: Int): Any? {
            return UNSAFE.getObjectVolatile(_data, byteOffset(index * 2 + 1))
        }
        inline fun getCont(index: Int): Any? {
            return UNSAFE.getObject(_data, byteOffset(index * 2 + 1))
        }
        inline fun casCont(index: Int, expect: Any?, update: Any): Boolean {
            return UNSAFE.compareAndSwapObject(_data, byteOffset(index * 2 + 1), expect, update)
        }
    }
    // These head and tail nodes are managed similar to MS queue.
    // In order to determine deque and enqueue locations, `_deqIdx`
    // and `_enqIdx` should be used.
    private val _head: AtomicRef<Node>
    private val _tail: AtomicRef<Node>

    internal fun head() = _head.value
    internal fun tail() = _tail.value

    // Indexes for deque and enqueue operations on the waiting queue,
    // which indicate positions for deque and enqueue, and their
    // equality means that the waiting queue is empty. These indexes are global,
    // therefore `idx div segmentSize` specifies a node id, while `idx mod segmentSize`
    // specifies an index in the node.
    private val _highest = atomic(0L)
    private val _lowest  = atomic(0L)
    private val _lock = atomic(0)

    init {
        // Initialize queue with empty node similar to MS queue
        // algorithm, but this node is just empty, not sentinel.
        val emptyNode = Node(0, null)
        _head = atomic(emptyNode)
        _tail = atomic(emptyNode)
    }

    override suspend fun send(element: E) {
        // Try to send without suspending at first,
        // invoke suspend implementation if it is not succeed.
        var t = 0
        while (true) {
            if (t >= 5) break

            var head = head()

            var h1 = _highest.value
            var l = _lowest.value
            var h2 = _highest.value
            while (h1 != h2) {
                h1 = _highest.value
                l = _lowest.value
                h2 = _highest.value
            }

            val ls = l ushr _counterOffset
            val lr = l and _counterMask
            val hs = h1 ushr _counterOffset
            val hr = h1 and _counterMask
            var s = ls + (hs shl (_counterOffset - 1))
            val r = lr + (hr shl (_counterOffset - 1))

            if (s + 1 <= r) {
                if (!_lowest.compareAndSet(l, l + (1L shl _counterOffset))) {
                    t++
                    continue
                }
                s++
                val i = (s % segmentSize).toInt()
                head = getHead(s / segmentSize, head)
                val el = readElement(head, i)
                if (el == BROKEN) continue
                head.putElement(i, BROKEN)

                val cont = head.getCont(i)
                head.putCont(i, null)

                if (cont == TAKEN_CONTINUATION) continue

                if (cont is CancellableContinuation<*>) {
                    cont as CancellableContinuation<in E>
                    if (!cont.tryResumeCont(element)) {
                        continue
                    }
                    return
                } else {
                    cont as SelectInstance<*>
                    val trySelectRes = cont.trySelect(this, element!!, null)
                    if (trySelectRes != SelectInstance.TRY_SELECT_SUCCESS) {
                        continue
                    }
                    return
                }
            } else break
        }

        suspendAtomicCancellableCoroutine<Unit>(holdCancellability = true) sc@ { curCont ->
            while (true) {
                var head = head()
                var tail = tail()

                _lock.incrementAndGet()
                while (_lock.value > wlocked) {}
                val l = _lowest.addAndGet(1L shl _counterOffset)
                val h1 = _highest.value
                _lock.decrementAndGet()

//                val h1 = _highest.value
//                val l = _lowest.addAndGet(1L shl _counterOffset)
//                val h2 = _highest.value
//                check(h1 == h2)

                val ls = l ushr  _counterOffset
                val lr = l and _counterMask
                val hs = h1 ushr  _counterOffset
                val hr = h1 and _counterMask
                val s = ls + (hs shl (_counterOffset - 1))
                val r = lr + (hr shl (_counterOffset - 1))

                if (ls > _minOverflowedValue) error(":(")

                val i = (s % segmentSize).toInt()
                if (s <= r) {
                    head = getHead(s / segmentSize, head)
                    val el = readElement(head, i)
                    if (el == BROKEN) continue
                    head.putElement(i, BROKEN)

                    val cont = head.getCont(i)
                    head.putCont(i, null)

                    if (cont == TAKEN_CONTINUATION) continue

                    if (cont is CancellableContinuation<*>) {
                        cont as CancellableContinuation<in E>
                        if (!cont.tryResumeCont(element)) {
                            continue
                        }
                        curCont.resume(Unit)
                        return@sc
                    } else {
                        cont as SelectInstance<*>
                        val trySelectRes = cont.trySelect(this, element!!, null)
                        if (trySelectRes != SelectInstance.TRY_SELECT_SUCCESS) {
                            continue
                        }
                        curCont.resume(Unit)
                        return@sc
                    }
                } else {
                    tail = getTail(s / segmentSize, tail)
                    tail.putCont(i, curCont)
                    if (!tail.casElement(i, null, element!!)) {
                        tail.putCont(i, null)
                        continue
                    }
                    curCont.initCancellability()
                    return@sc
                }
            }
        }
    }

    override suspend fun receive(): E {
        // Try to send without suspending at first,
        // invoke suspend implementation if it is not succeed.
        var t = 0
        while (true) {
            if (t >= 5) break

            var head = head()

            var h1 = _highest.value
            var l = _lowest.value
            var h2 = _highest.value
            while (h1 != h2) {
                h1 = _highest.value
                l = _lowest.value
                h2 = _highest.value
            }

            val ls = l ushr _counterOffset
            val lr = l and _counterMask
            val hs = h1 ushr _counterOffset
            val hr = h1 and _counterMask
            val s = ls + (hs shl (_counterOffset - 1))
            var r = lr + (hr shl (_counterOffset - 1))

            if (r + 1 <= s) {
                if (!_lowest.compareAndSet(l, l + 1L)) {
                    t++
                    continue
                }
                r++
                val i = (r % segmentSize).toInt()
                head = getHead(r / segmentSize, head)
                val el = readElement(head, i)
                if (el == BROKEN) continue
                head.putElement(i, BROKEN)

                val cont = head.getCont(i)
                head.putCont(i, null)

                if (cont == TAKEN_CONTINUATION) continue

                if (cont is CancellableContinuation<*>) {
                    cont as CancellableContinuation<Unit>
                    if (!cont.tryResumeCont(Unit)) continue
                    return el as E
                } else {
                    cont as SelectInstance<*>
                    if (cont.trySelect(this, RECEIVER_ELEMENT, null) != SelectInstance.TRY_SELECT_SUCCESS) continue
                    return el as E
                }
            } else break
        }



        return suspendAtomicCancellableCoroutine(holdCancellability = true) sc@{ curCont ->
            while (true) {
                var head = head()
                var tail = tail()

                _lock.incrementAndGet()
                while (_lock.value > wlocked) {}
                val l = _lowest.addAndGet(1L)
                val h1 = _highest.value
                _lock.decrementAndGet()

//                val h1 = _highest.value
//                val l = _lowest.addAndGet(1L)
//                val h2 = _highest.value
//                check(h1 == h2)

                val ls = l ushr _counterOffset
                val lr = l and _counterMask
                val hs = h1 ushr _counterOffset
                val hr = h1 and _counterMask
                val s = ls + (hs shl (_counterOffset - 1))
                val r = lr + (hr shl (_counterOffset - 1))

                if (lr > _minOverflowedValue) error(":(")


                val i = (r % segmentSize).toInt()
                if (r <= s) {
                    head = getHead(r / segmentSize, head)
                    val el = readElement(head, i)
                    if (el == BROKEN) continue
                    head.putElement(i, BROKEN)

                    val cont = head.getCont(i)
                    head.putCont(i, null)

                    if (cont == TAKEN_CONTINUATION) continue

                    if (cont is CancellableContinuation<*>) {
                        cont as CancellableContinuation<Unit>
                        if (!cont.tryResumeCont(Unit)) continue
                        curCont.resume(el as E)
                        return@sc
                    } else {
                        cont as SelectInstance<*>
                        if (cont.trySelect(this, RECEIVER_ELEMENT, null) != SelectInstance.TRY_SELECT_SUCCESS) continue
                        curCont.resume(el as E)
                        return@sc
                    }
                } else {
                    tail = getTail(r / segmentSize, tail)
                    tail.putCont(i, curCont)
                    if (!tail.casElement(i, null, RECEIVER_ELEMENT)) {
                        tail.putCont(i, null)
                        continue
                    }
                    curCont.initCancellability()
                    return@sc
                }
            }
        }
    }

    override fun offer(element: E): Boolean {
        return false
    }

    override fun poll(): E? {
        return null
    }

    // Tries to read an element from the specified node
    // at the specified index. Returns the read element or
    // marks the slot as broken (sets `BROKEN` to the slot)
    // and returns `BROKEN` if the element is unavailable.
    private fun readElement(node: Node, index: Int): Any {
        // Spin wait on the slot.
        val element = node.getElementVolatile(index)
        if (element != null) return element
        // Cannot spin forever, mark the slot as broken if it is still unavailable.
        return if (node.casElement(index, null, BROKEN)) {
            BROKEN
        } else {
            // The element is set, read it and return.
            node.getElementVolatile(index)!!
        }
    }








    private fun getHead(id: Long, cur: Node): Node {
        if (cur.id == id) { return cur }
        val cur = findOrCreateNode(id, cur)
        cur.putPrev(null)
        moveHeadForward(cur)
        return cur
    }

    private fun getTail(id: Long, cur: Node): Node {
        return findOrCreateNode(id, cur)
    }

    private fun findOrCreateNode(id: Long, cur: Node): Node {
        var cur = cur
        while (cur.id < id) {
            var curNext = cur.next()
            if (curNext == null) {
                // add new segment
                val newTail = Node(cur.id + 1, cur)
                if (cur.casNext(null, newTail)) {
                    if (cur.removed) { cur.remove() }
                    moveTailForward(newTail)
                    curNext = newTail
                } else {
                    curNext = cur.next()
                }
            }
            cur = curNext!!
        }
        return cur
    }

    private fun moveHeadForward(new: Node) {
        while(true) {
            val cur = head()
            if (cur.id > new.id) return
            if (_head.compareAndSet(cur, new)) return
        }
    }

    private fun moveTailForward(new: Node) {
        while(true) {
            val cur = tail()
            if (cur.id > new.id) return
            if (_tail.compareAndSet(cur, new)) return
        }
    }




    override val onSend: Param1RegInfo<Unit>
        get() = Param1RegInfo<Unit>(this, RendezvousChannel<*>::regSelectSend, Companion::actOnSendAndOnReceive)
    override val onReceive: Param0RegInfo<E>
        get() = Param0RegInfo<E>(this, RendezvousChannel<*>::regSelectReceive, Companion::actOnSendAndOnReceive)

    private fun regSelectSend(selectInstance: SelectInstance<*>, element: Any?): RegResult? {
        while (true) {
            var head = head()
            var tail = tail()

            _lock.incrementAndGet()
            while (_lock.value > wlocked) {}
            val l = _lowest.addAndGet(1L shl _counterOffset)
            val h1 = _highest.value
            _lock.decrementAndGet()

//            val h1 = _highest.value
//            val l = _lowest.addAndGet(1L shl _counterOffset)
//            val h2 = _highest.value
//            check(h1 == h2)

            val ls = l ushr  _counterOffset
            val lr = l and _counterMask
            val hs = h1 ushr  _counterOffset
            val hr = h1 and _counterMask
            val s = ls + (hs shl (_counterOffset - 1))
            val r = lr + (hr shl (_counterOffset - 1))

            if (ls > _minOverflowedValue) error(":(")

            val i = (s % segmentSize).toInt()
            if (s <= r) {
                head = getHead(s / segmentSize, head)
                val el = readElement(head, i)
                if (el == BROKEN) continue
                head.putElement(i, BROKEN)

                val cont = head.getCont(i)
                head.putCont(i, null)

                if (cont == TAKEN_CONTINUATION) continue

                if (cont is CancellableContinuation<*>) {
                    cont as CancellableContinuation<in E>
                    if (!cont.tryResumeCont(element as E)) { continue }
                    selectInstance.setState(RECEIVER_ELEMENT)
                    return null
                } else {
                    cont as SelectInstance<*>
                    val status = cont.trySelect(this, element!!, selectInstance)
                    if (status == SelectInstance.TRY_SELECT_FAIL) continue
                    if (status == SelectInstance.TRY_SELECT_SUCCESS) {
                        selectInstance.setState(RECEIVER_ELEMENT)
                        return null
                    }
                    if (status == SelectInstance.TRY_SELECT_CONFIRM) {
                        return SelectInstance.REG_RESULT_CONFIRMED
                    }
                }
            } else {
                tail = getTail(s / segmentSize, tail)
                tail.putCont(i, selectInstance)
                if (!tail.casElement(i, null, element!!)) {
                    tail.putCont(i, null)
                    continue
                }
                return RegResult(tail, i)
            }
        }
    }

    private fun regSelectReceive(selectInstance: SelectInstance<*>, element: Any?): RegResult? {
        while (true) {
            var head = head()
            var tail = tail()

            _lock.incrementAndGet()
            while (_lock.value > wlocked) {}
            val l = _lowest.addAndGet(1L)
            val h1 = _highest.value
            _lock.decrementAndGet()

//            val h1 = _highest.value
//            val l = _lowest.addAndGet(1L)
//            val h2 = _highest.value
//            check(h1 == h2)

            val ls = l ushr _counterOffset
            val lr = l and _counterMask
            val hs = h1 ushr _counterOffset
            val hr = h1 and _counterMask
            val s = ls + (hs shl (_counterOffset - 1))
            val r = lr + (hr shl (_counterOffset - 1))

            if (lr > _minOverflowedValue) error(":(")


            val i = (r % segmentSize).toInt()

            if (r <= s) {
                head = getHead(r / segmentSize, head)
                val el = readElement(head, i)
                if (el == BROKEN) continue
                head.putElement(i, BROKEN)

                val cont = head.getCont(i)
                head.putCont(i, null)

                if (cont == TAKEN_CONTINUATION) continue

                if (cont is CancellableContinuation<*>) {
                    cont as CancellableContinuation<Unit>
                    if (!cont.tryResumeCont(Unit)) continue
                    selectInstance.setState(el)
                    return null
                } else {
                    cont as SelectInstance<*>
                    val status = cont.trySelect(this, RECEIVER_ELEMENT, selectInstance)
                    if (status == SelectInstance.TRY_SELECT_FAIL) continue
                    if (status == SelectInstance.TRY_SELECT_SUCCESS) {
                        selectInstance.setState(el)
                        return null
                    }
                    if (status == SelectInstance.TRY_SELECT_CONFIRM) {
                        return SelectInstance.REG_RESULT_CONFIRMED
                    }
                }
            } else {
                tail = getTail(r / segmentSize, tail)
                tail.putCont(i, selectInstance)
                if (!tail.casElement(i, null, RECEIVER_ELEMENT)) {
                    tail.putCont(i, null)
                    continue
                }
                return RegResult(tail, i)
            }
        }
    }

    internal companion object {
        @JvmField val UNSAFE = UtilUnsafe.unsafe
        @JvmField val base = UNSAFE.arrayBaseOffset(Array<Any>::class.java)
        @JvmField val shift = 31 - Integer.numberOfLeadingZeros(UNSAFE.arrayIndexScale(Array<Any>::class.java))
        @JvmStatic inline fun byteOffset(i: Int) = (i.toLong() shl shift) + base
        @JvmStatic inline fun nodeId(globalIdx: Long, segmentSize: Int) = globalIdx / segmentSize
        @JvmStatic inline fun indexInNode(globalIdx: Long, segmentSize: Int) = (globalIdx % segmentSize).toInt()
        @JvmField val RECEIVER_ELEMENT = Any()
        @JvmField val TAKEN_CONTINUATION = Any()
        @JvmField val BROKEN = Any()
        @JvmStatic private fun <FUNC_RESULT> actOnSendAndOnReceive(result: Any?, block: (FUNC_RESULT) -> Any?): Any? {
            when {
                result === RECEIVER_ELEMENT -> return block(Unit as FUNC_RESULT)
                else -> return block(result as FUNC_RESULT)
            }
        }
    }
}
const val segmentSize = 32

const val _counterOffset = 30 // 32
const val _counterMask = (1L shl _counterOffset) - 1L
const val _minOverflowedValue = 1L shl (_counterOffset - 1)

const val wlocked = 1 shl 29