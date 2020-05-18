package com.baidu.fsg.uid.business;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.springframework.util.Assert;

/**
 * Allocate 29 bits for the UID(long)<br>
 *
 * @author yutianbao
 */
public class BusinessBitsAllocator {
    /**
     * Total 64 bits
     */
    public static final int TOTAL_BITS = 29;

    /**
     * Bits for [date-> hignSeq-> workerId-> appId-> bizType-> extraTag-> shardingId-> sequence]
     */
    private final int dateBits = 8;
    private final int highSeqBits;
    private final int workerIdBits;
    private final int appIdBits;
    private final int bizTypeBits;
    private final int extraTagBits;
    private final int shardingIdBits;
    private final int sequenceBits;

    /**
     * Max value
     */
    private final long maxHignSeq;
    private final long maxWorkerId;
    private final long maxAppId;
    private final long maxBizType;
    private final long maxExtraTag;
    private final long maxShardingId;
    private final long maxSequence;


    private final int highSeqBegin;
    private final int workerIdBegin;
    private final int appIdBegin;
    private final int bizTypeBegin;
    private final int extraTagBegin;
    private final int shardingIdBegin;
    private final int sequenceBegin;

    /**
     * Constructor
     */
    public BusinessBitsAllocator(int highSeqBits, int workerIdBits, int appIdBits, int bizTypeBits, int extraTagBits, int shardingIdBits, int sequenceBits) {
        int allocateTotalBits = dateBits + highSeqBits + workerIdBits + appIdBits + bizTypeBits + extraTagBits + shardingIdBits + sequenceBits;
        Assert.isTrue(allocateTotalBits == TOTAL_BITS, "allocate not enough 29 bits");

        /** bits */
        this.highSeqBits = highSeqBits;
        this.workerIdBits = workerIdBits;
        this.appIdBits = appIdBits;
        this.bizTypeBits = bizTypeBits;
        this.extraTagBits = extraTagBits;
        this.shardingIdBits = shardingIdBits;
        this.sequenceBits = sequenceBits;

        /** max */
        this.maxHignSeq = calcMaxVal(highSeqBits);
        this.maxWorkerId = calcMaxVal(workerIdBits);
        this.maxAppId = calcMaxVal(appIdBits);
        this.maxBizType = calcMaxVal(bizTypeBits);
        this.maxExtraTag = calcMaxVal(extraTagBits);
        this.maxShardingId = calcMaxVal(shardingIdBits);
        this.maxSequence = calcMaxVal(sequenceBits);

        /** begin */
        this.highSeqBegin = dateBits;
        this.workerIdBegin = highSeqBegin + highSeqBits;
        this.appIdBegin = workerIdBegin + workerIdBits;
        this.bizTypeBegin = appIdBegin + appIdBits;
        this.extraTagBegin = bizTypeBegin + bizTypeBits;
        this.shardingIdBegin = extraTagBegin + extraTagBits;
        this.sequenceBegin = shardingIdBegin + shardingIdBits;
    }

    /**
     * Allocate bits
     *
     * @return
     */
    public String allocate(String date, long hignSeq, long workerId, long appId, long bizType, long extraTag, long shardingId, long sequence) {
        return new StringBuilder(date)
                .append(fillZero(hignSeq, highSeqBits))
                .append(fillZero(workerId, workerIdBits))
                .append(fillZero(appId, appIdBits))
                .append(fillZero(bizType, bizTypeBits))
                .append(fillZero(extraTag, extraTagBits))
                .append(fillZero(shardingId, shardingIdBits))
                .append(fillZero(sequence, sequenceBits))
                .toString();
    }

    public String allocate(String date, long hignSeq, long workerId, long sequence) {
        return new StringBuilder(date)
                .append(fillZero(hignSeq, highSeqBits))
                .append(fillZero(workerId, workerIdBits))
                .append("%s%s%s%s")
                .append(fillZero(sequence, sequenceBits))
                .toString();
    }

    public String complete(String uid, long appId, long bizType, long extraTag, long shardingId) {
        return String.format(uid,
                fillZero(appId, appIdBits),
                fillZero(bizType, bizTypeBits),
                fillZero(extraTag, extraTagBits),
                fillZero(shardingId, shardingIdBits));
    }

    public static String fillZero(long bitVal, int bit) {
        StringBuilder result = new StringBuilder(String.valueOf(bitVal));
        while (result.length() < bit) {
            result.insert(0, "0");
        }
        return result.toString();
    }

    private long calcMaxVal(int bits) {
        if (bits >= 18) {
            throw new IllegalArgumentException("最大值超过Long.MAX_VALUE(9223372036854775807)");
        }

        StringBuilder max = new StringBuilder();
        for (int i = 0; i < bits; i++) {
            max.append(9);
        }
        return Long.valueOf(max.toString());
    }

    /**
     * Getters
     */
    public int getDateBits() {
        return dateBits;
    }

    public int getHighSeqBits() {
        return highSeqBits;
    }

    public int getWorkerIdBits() {
        return workerIdBits;
    }

    public int getAppIdBits() {
        return appIdBits;
    }

    public int getBizTypeBits() {
        return bizTypeBits;
    }

    public int getExtraTagBits() {
        return extraTagBits;
    }

    public int getShardingIdBits() {
        return shardingIdBits;
    }

    public int getSequenceBits() {
        return sequenceBits;
    }

    public long getMaxHignSeq() {
        return maxHignSeq;
    }

    public long getMaxWorkerId() {
        return maxWorkerId;
    }

    public long getMaxAppId() {
        return maxAppId;
    }

    public long getMaxBizType() {
        return maxBizType;
    }

    public long getMaxExtraTag() {
        return maxExtraTag;
    }

    public long getMaxShardingId() {
        return maxShardingId;
    }

    public long getMaxSequence() {
        return maxSequence;
    }

    public int getHighSeqBegin() {
        return highSeqBegin;
    }

    public int getWorkerIdBegin() {
        return workerIdBegin;
    }

    public int getAppIdBegin() {
        return appIdBegin;
    }

    public int getBizTypeBegin() {
        return bizTypeBegin;
    }

    public int getExtraTagBegin() {
        return extraTagBegin;
    }

    public int getShardingIdBegin() {
        return shardingIdBegin;
    }

    public int getSequenceBegin() {
        return sequenceBegin;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
