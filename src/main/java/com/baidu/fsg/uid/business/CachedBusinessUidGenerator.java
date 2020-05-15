package com.baidu.fsg.uid.business;

import com.baidu.fsg.uid.BitsAllocator;
import com.baidu.fsg.uid.buffer.BufferPaddingExecutor;
import com.baidu.fsg.uid.buffer.RejectedPutBufferHandler;
import com.baidu.fsg.uid.buffer.RejectedTakeBufferHandler;
import com.baidu.fsg.uid.buffer.RingBuffer;
import com.baidu.fsg.uid.exception.UidGenerateException;
import com.baidu.fsg.uid.impl.CachedUidGenerator;
import com.baidu.fsg.uid.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

public class CachedBusinessUidGenerator extends BusinessUidGenerator implements DisposableBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(CachedUidGenerator.class);
    private static final int DEFAULT_BOOST_POWER = 3;

    static final int tableSizeFor(int var0) {
        int var1 = var0 - 1;
        var1 |= var1 >>> 1;
        var1 |= var1 >>> 2;
        var1 |= var1 >>> 4;
        var1 |= var1 >>> 8;
        var1 |= var1 >>> 16;
        return var1 < 0 ? 1 : (var1 >= 1073741824 ? 1073741824 : var1 + 1);
    }

    /**
     * Spring properties
     */
    private int boostPower = DEFAULT_BOOST_POWER;
    private int paddingFactor = RingBuffer.DEFAULT_PADDING_PERCENT;
    private Long scheduleInterval;

    private RejectedPutBufferHandler<String> rejectedPutBufferHandler;
    private RejectedTakeBufferHandler rejectedTakeBufferHandler;

    /**
     * RingBuffer
     */
    private RingBuffer<String> ringBuffer;
    private BufferPaddingExecutor<String> bufferPaddingExecutor;

    @Override
    public void afterPropertiesSet() throws Exception {
        // initialize workerId & bitsAllocator
        super.afterPropertiesSet();

        // initialize RingBuffer & RingBufferPaddingExecutor
        this.initRingBuffer();
        LOGGER.info("Initialized RingBuffer successfully.");
    }

    public String getUID() {
        try {
            return ringBuffer.take();
        } catch (Exception e) {
            LOGGER.error("Generate unique id exception. ", e);
            throw new UidGenerateException(e);
        }
    }

    @Override
    public String parseUID(String uid) {
        return super.parseUID(uid);
    }

    @Override
    public void destroy() throws Exception {
        bufferPaddingExecutor.shutdown();
    }

    /**
     * Get the UIDs in the same specified second under the max sequence
     *
     * @param currentSecond
     * @return UID list, size of {@link BitsAllocator#getMaxSequence()} + 1
     */
    protected List<String> nextIdsForOneSecond(long currentSecond) {
        String currentDate;
        synchronized (this) {
            currentDate = DateUtils.getCurrentDayByDaySimplePattern();
            int compareRes = currentDate.compareTo(lastDate);
            if (compareRes < 0) {
                // 时间拨回
                throw new UidGenerateException("Clock moved backwards. Refusing UID generate. ");
            } else if (compareRes > 0) {
                // 次日重置
                highSeq = 0;
                lastDate = currentDate;
            } else {
                highSeq++;
                // TODO 校验highSeq是否已大于最大值
            }
        }

        // Initialize result list size of (max sequence + 1)
        int listSize = (int) bitsAllocator.getMaxSequence() + 1;
        List<String> uidList = new ArrayList<>(listSize);

        // Allocate the first sequence of the second, the others can be calculated with the offset
        String firstSeqUid = bitsAllocator.allocate(currentDate, highSeq, workerId, 0L);
        String prefix = firstSeqUid.substring(0, firstSeqUid.length() - bitsAllocator.getSequenceBits());
        for (int offset = 0; offset < bitsAllocator.getMaxSequence(); offset++) {
            String s = prefix + BusinessBitsAllocator.fillZero(offset, bitsAllocator.getSequenceBits());
            uidList.add(s);
        }

        return uidList;
    }

    /**
     * Initialize RingBuffer & RingBufferPaddingExecutor
     */
    private void initRingBuffer() {
        // initialize RingBuffer
        int bufferSize = ((int) bitsAllocator.getMaxSequence() + 1) << boostPower;
        this.ringBuffer = new RingBuffer(tableSizeFor(bufferSize), paddingFactor);
        LOGGER.info("Initialized ring buffer size:{}, paddingFactor:{}", bufferSize, paddingFactor);

        // initialize RingBufferPaddingExecutor
        boolean usingSchedule = (scheduleInterval != null);
        this.bufferPaddingExecutor = new BufferPaddingExecutor(ringBuffer, this::nextIdsForOneSecond, usingSchedule);
        if (usingSchedule) {
            bufferPaddingExecutor.setScheduleInterval(scheduleInterval);
        }

        LOGGER.info("Initialized BufferPaddingExecutor. Using schdule:{}, interval:{}", usingSchedule, scheduleInterval);

        // set rejected put/take handle policy
        this.ringBuffer.setBufferPaddingExecutor(bufferPaddingExecutor);
        if (rejectedPutBufferHandler != null) {
            this.ringBuffer.setRejectedPutHandler(rejectedPutBufferHandler);
        }
        if (rejectedTakeBufferHandler != null) {
            this.ringBuffer.setRejectedTakeHandler(rejectedTakeBufferHandler);
        }

        // fill in all slots of the RingBuffer
        bufferPaddingExecutor.paddingBuffer();

        // start buffer padding threads
        bufferPaddingExecutor.start();
    }

    /**
     * Setters for spring property
     */
    public void setBoostPower(int boostPower) {
        Assert.isTrue(boostPower > 0, "Boost power must be positive!");
        this.boostPower = boostPower;
    }

    public void setRejectedPutBufferHandler(RejectedPutBufferHandler rejectedPutBufferHandler) {
        Assert.notNull(rejectedPutBufferHandler, "RejectedPutBufferHandler can't be null!");
        this.rejectedPutBufferHandler = rejectedPutBufferHandler;
    }

    public void setRejectedTakeBufferHandler(RejectedTakeBufferHandler rejectedTakeBufferHandler) {
        Assert.notNull(rejectedTakeBufferHandler, "RejectedTakeBufferHandler can't be null!");
        this.rejectedTakeBufferHandler = rejectedTakeBufferHandler;
    }

    public void setScheduleInterval(long scheduleInterval) {
        Assert.isTrue(scheduleInterval > 0, "Schedule interval must positive!");
        this.scheduleInterval = scheduleInterval;
    }


    public static void main(String[] args) throws Exception {
        CachedBusinessUidGenerator generator = new CachedBusinessUidGenerator();
        generator.setWorkerIdAssigner(() -> 15L);
        generator.afterPropertiesSet();
    }
}