package com.lanrhyme.micyou.network

import com.lanrhyme.micyou.AudioPacketMessage
import com.lanrhyme.micyou.AudioPacketMessageOrdered
import com.lanrhyme.micyou.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * 音频抖动缓冲区
 * 用于处理UDP乱序包，保持音频播放顺序
 * 设计目标：低延迟 + 正确的播放顺序
 *
 * 新增特性：
 * - 等待窗口：检测到 gap 后等待一段时间，给乱序包到达的机会
 * - FEC 恢复：等待超时后尝试使用前向纠错包恢复丢失的音频数据
 */
class JitterBuffer(
    private val onAudioPacketReady: suspend (AudioPacketMessage) -> Unit,
    private val waitWindowMs: Long = 30L,
    private val fecGroupSize: Int = 12
) {
    // 使用ConcurrentHashMap存储包，key为sequenceNumber
    private val buffer = ConcurrentHashMap<Int, AudioPacketMessageOrdered>()

    // FEC 包存储，key为fecSequenceNumber（组起始序列号）
    private val fecPackets = ConcurrentHashMap<Int, AudioPacketMessageOrdered>()

    // 下一个期望播放的序列号
    private val expectedSequenceNumber = AtomicInteger(0)

    // 是否已初始化（收到第一个包）
    private val initialized = AtomicInteger(0)

    // 统计信息
    private val packetsReceived = AtomicInteger(0)
    private val packetsLost = AtomicInteger(0)
    private val packetsOutOfOrder = AtomicInteger(0)
    private val fecRecovered = AtomicInteger(0)
    private val fecFailed = AtomicInteger(0)
    private val fecReceived = AtomicInteger(0)

    @Volatile
    private var isRunning = false

    // 当前等待中的 gap 补偿任务
    @Volatile
    private var pendingGapJob: Job? = null

    // 当前等待的缺失序列号
    @Volatile
    private var gapDetectedSeq: Int = -1

    // 协程作用域用于异步播放音频包和 gap 等待
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    /**
     * 插入音频包
     */
    fun insert(packet: AudioPacketMessageOrdered) {
        if (!isRunning) return

        // FEC 包：存入 fecPackets，不放入主 buffer
        if (packet.fecSequenceNumber >= 0) {
            fecReceived.incrementAndGet()
            fecPackets[packet.fecSequenceNumber] = packet
            Logger.d("JitterBuffer", "FEC packet received for group starting at ${packet.fecSequenceNumber}")
            return
        }

        packetsReceived.incrementAndGet()

        // 初始化：第一个包设置期望序列号
        if (initialized.get() == 0) {
            expectedSequenceNumber.set(packet.sequenceNumber)
            initialized.set(1)
        }

        // 检查是否是过期包
        val currentExpected = expectedSequenceNumber.get()
        if (packet.sequenceNumber < currentExpected) {
            packetsOutOfOrder.incrementAndGet()
            return // 丢弃过期包
        }

        // 存储包
        buffer[packet.sequenceNumber] = packet

        // 如果正在等待 gap 且这个包正好填补了 gap，取消等待并立即处理
        if (gapDetectedSeq >= 0 && packet.sequenceNumber == gapDetectedSeq) {
            pendingGapJob?.cancel()
            pendingGapJob = null
            gapDetectedSeq = -1
            Logger.d("JitterBuffer", "Gap filled by arriving packet ${packet.sequenceNumber}")
        }

        // 尝试播放连续的包
        processBuffer()
    }

    /**
     * 处理缓冲区，播放连续的包
     */
    private fun processBuffer() {
        while (isRunning) {
            val seqNum = expectedSequenceNumber.get()
            val packet = buffer.remove(seqNum) ?: break // 没有期望的包，退出循环

            // 使用协程异步播放包
            val packetToPlay = packet
            scope.launch {
                try {
                    onAudioPacketReady(packetToPlay.audioPacket)
                } catch (e: Exception) {
                    Logger.e("JitterBuffer", "Error playing audio packet: ${e.message}")
                }
            }

            // 期望下一个包
            expectedSequenceNumber.incrementAndGet()
        }

        // 检查是否有包丢失（缓冲区中有更高序列号的包）
        val nextExpected = expectedSequenceNumber.get()
        val nextAvailable = buffer.keys.minOrNull()
        if (nextAvailable != null && nextAvailable > nextExpected) {
            // 如果已经在等待同一个 gap，不重复启动
            if (gapDetectedSeq == nextExpected && pendingGapJob?.isActive == true) {
                return
            }

            // 有包丢失，启动等待窗口
            gapDetectedSeq = nextExpected
            val lostCount = nextAvailable - nextExpected

            pendingGapJob?.cancel()
            pendingGapJob = scope.launch {
                Logger.d("JitterBuffer", "Waiting ${waitWindowMs}ms for gap at seq $nextExpected (potential loss: $lostCount packets)")
                delay(waitWindowMs)

                // 等待超时，尝试 FEC 恢复
                val recovered = tryFecRecovery(nextExpected)
                if (recovered != null) {
                    // FEC 恢复成功，播放恢复的包
                    Logger.i("JitterBuffer", "FEC recovered packet at seq $nextExpected")
                    fecRecovered.incrementAndGet()
                    try {
                        onAudioPacketReady(recovered)
                    } catch (e: Exception) {
                        Logger.e("JitterBuffer", "Error playing FEC recovered packet: ${e.message}")
                    }
                    expectedSequenceNumber.incrementAndGet()
                    gapDetectedSeq = -1
                    pendingGapJob = null
                    // 继续处理后续连续的包
                    processBuffer()
                } else {
                    // FEC 恢复失败，跳过丢失的包
                    packetsLost.addAndGet(lostCount)
                    Logger.d("JitterBuffer", "Lost $lostCount packets, skipping to $nextAvailable")
                    fecFailed.incrementAndGet()
                    expectedSequenceNumber.set(nextAvailable)
                    gapDetectedSeq = -1
                    pendingGapJob = null
                    // 重新处理缓冲区
                    processBuffer()
                }
            }
        }
    }

    /**
     * 尝试使用 FEC 恢复丢失的包
     * @param missingSeq 丢失的序列号
     * @return 恢复的 AudioPacketMessage，如果无法恢复则返回 null
     */
    private fun tryFecRecovery(missingSeq: Int): AudioPacketMessage? {
        // 计算该序列号所属的 FEC 组起始位置
        val groupStart = (missingSeq / fecGroupSize) * fecGroupSize
        val fecPacket = fecPackets[groupStart] ?: return null

        // 收集该组内已收到的包（不含丢失的）
        val receivedInGroup = mutableListOf<ByteArray>()
        for (seq in groupStart until groupStart + fecGroupSize) {
            if (seq == missingSeq) continue
            val pkt = buffer[seq]
            if (pkt != null) {
                receivedInGroup.add(pkt.audioPacket.buffer)
            } else {
                // 该组内还有其他丢失的包，无法恢复
                return null
            }
        }

        // 只有当该组恰好丢了 1 个包时才能恢复
        if (receivedInGroup.size != fecGroupSize - 1) {
            return null
        }

        // XOR FEC buffer 与所有已收到包的 buffer
        val fecBuffer = fecPacket.audioPacket.buffer
        val recoveredBuffer = xorBuffers(listOf(fecBuffer) + receivedInGroup)

        // 从组内任意一个已收到的包获取音频参数
        val referencePacket = buffer[groupStart]?.audioPacket
            ?: buffer[groupStart + 1]?.audioPacket
            ?: return null

        return AudioPacketMessage(
            buffer = recoveredBuffer,
            sampleRate = referencePacket.sampleRate,
            channelCount = referencePacket.channelCount,
            audioFormat = referencePacket.audioFormat
        )
    }

    /**
     * XOR 多个 buffer（处理不同长度：以最长的为准，短的用 0 填充）
     */
    private fun xorBuffers(buffers: List<ByteArray>): ByteArray {
        val maxLen = buffers.maxOf { it.size }
        val result = ByteArray(maxLen)
        for (buf in buffers) {
            for (i in buf.indices) {
                result[i] = (result[i].toInt() xor buf[i].toInt()).toByte()
            }
            // 超出 buf 长度的部分视为 0，XOR 不改变 result（0 XOR x = x）
        }
        return result
    }

    /**
     * 启动缓冲区
     */
    fun start() {
        isRunning = true
        Logger.i("JitterBuffer", "Jitter buffer started (waitWindow=${waitWindowMs}ms, fecGroupSize=$fecGroupSize)")
    }

    /**
     * 停止缓冲区
     */
    fun stop() {
        isRunning = false
        pendingGapJob?.cancel()
        pendingGapJob = null
        gapDetectedSeq = -1
        buffer.clear()
        fecPackets.clear()
        expectedSequenceNumber.set(0)
        initialized.set(0)
        Logger.i("JitterBuffer", "Jitter buffer stopped")
    }

    /**
     * 获取统计信息
     */
    fun getStats(): String {
        return "received=${packetsReceived.get()}, lost=${packetsLost.get()}, outOfOrder=${packetsOutOfOrder.get()}, " +
                "fecReceived=${fecReceived.get()}, fecRecovered=${fecRecovered.get()}, fecFailed=${fecFailed.get()}, buffer=${buffer.size}"
    }
}
