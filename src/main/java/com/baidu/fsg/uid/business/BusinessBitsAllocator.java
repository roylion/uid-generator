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
     * Bits for [date-> hignSeq-> workerId-> systemId-> bizId-> extraInfo-> sharding-> sequence]
     */
    private final int dateBits = 8;
    private final int highSeqBits;
    private final int workerIdBits;
    private final int systemIdBits;
    private final int bizIdBits;
    private final int extraInfoBits;
    private final int shardingBits;
    private final int sequenceBits;

    /**
     * Max value
     */
    private final long maxHignSeq;
    private final long maxWorkerId;
    private final long maxSystemId;
    private final long maxBizId;
    private final long maxExtraInfo;
    private final long maxSharding;
    private final long maxSequence;


    private final int highSeqBegin;
    private final int workerIdBegin;
    private final int systemIdBegin;
    private final int bizIdBegin;
    private final int extraInfoBegin;
    private final int shardingBegin;
    private final int sequenceBegin;

    /**
     * Constructor
     */
    public BusinessBitsAllocator(int highSeqBits, int workerIdBits, int systemIdBits, int bizIdBits, int extraInfoBits, int shardingBits, int sequenceBits) {
        int allocateTotalBits = dateBits + highSeqBits + workerIdBits + systemIdBits + bizIdBits + extraInfoBits + shardingBits + sequenceBits;
        Assert.isTrue(allocateTotalBits == TOTAL_BITS, "allocate not enough 29 bits");

        /** bits */
        this.highSeqBits = highSeqBits;
        this.workerIdBits = workerIdBits;
        this.systemIdBits = systemIdBits;
        this.bizIdBits = bizIdBits;
        this.extraInfoBits = extraInfoBits;
        this.shardingBits = shardingBits;
        this.sequenceBits = sequenceBits;

        /** max */
        this.maxHignSeq = calcMaxVal(highSeqBits);
        this.maxWorkerId = calcMaxVal(workerIdBits);
        this.maxSystemId = calcMaxVal(systemIdBits);
        this.maxBizId = calcMaxVal(bizIdBits);
        this.maxExtraInfo = calcMaxVal(extraInfoBits);
        this.maxSharding = calcMaxVal(shardingBits);
        this.maxSequence = calcMaxVal(sequenceBits);

        /** begin */
        this.highSeqBegin = dateBits;
        this.workerIdBegin = highSeqBegin + highSeqBits;
        this.systemIdBegin = workerIdBegin + workerIdBits;
        this.bizIdBegin = systemIdBegin + systemIdBits;
        this.extraInfoBegin = bizIdBegin + bizIdBits;
        this.shardingBegin = extraInfoBegin + extraInfoBits;
        this.sequenceBegin = shardingBegin + shardingBits;
    }

    /**
     * Allocate bits
     *
     * @return
     */
    public String allocate(String date, long hignSeq, long workerId, long systemId, long bizId, long extraInfo, long sharding, long sequence) {
        return new StringBuilder(date)
                .append(fillZero(hignSeq, highSeqBits))
                .append(fillZero(workerId, workerIdBits))
                .append(fillZero(systemId, systemIdBits))
                .append(fillZero(bizId, bizIdBits))
                .append(fillZero(extraInfo, extraInfoBits))
                .append(fillZero(sharding, shardingBits))
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

    public int getSystemIdBits() {
        return systemIdBits;
    }

    public int getBizIdBits() {
        return bizIdBits;
    }

    public int getExtraInfoBits() {
        return extraInfoBits;
    }

    public int getShardingBits() {
        return shardingBits;
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

    public long getMaxSystemId() {
        return maxSystemId;
    }

    public long getMaxBizId() {
        return maxBizId;
    }

    public long getMaxExtraInfo() {
        return maxExtraInfo;
    }

    public long getMaxSharding() {
        return maxSharding;
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

    public int getSystemIdBegin() {
        return systemIdBegin;
    }

    public int getBizIdBegin() {
        return bizIdBegin;
    }

    public int getExtraInfoBegin() {
        return extraInfoBegin;
    }

    public int getShardingBegin() {
        return shardingBegin;
    }

    public int getSequenceBegin() {
        return sequenceBegin;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
