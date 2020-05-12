package com.baidu.fsg.uid.business;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.springframework.util.Assert;

/**
 * Allocate 64 bits for the UID(long)<br>
 * sign (fixed 1bit) -> deltaSecond -> businessId -> workerId -> sequence(within the same second)
 *
 * @author yutianbao
 */
public class BusinessBitsAllocator {
    /**
     * Total 64 bits
     */
    public static final int TOTAL_BITS = 1 << 6;

    /**
     * Bits for [sign-> businessId-> second-> workId-> sequence]
     */
    private int signBits = 1;
    private final int timestampBits;
    private final int sourceBits;
    private final int typeBits;
    private final int extBits;
    private final int workerIdBits;
    private final int sequenceBits;

    /**
     * Max value for workId & sequence
     */
    private final long maxDeltaSeconds;
    private final long maxSource;
    private final long maxType;
    private final long maxExt;
    private final long maxWorkerId;
    private final long maxSequence;

    /**
     * Shift for timestamp & workerId
     */
    private final int timestampShift;
    private final int sourceShift;
    private final int typeShift;
    private final int extShift;
    private final int workerIdShift;

    /**
     * Constructor with timestampBits, workerIdBits, sequenceBits<br>
     * The highest bit used for sign, so <code>63</code> bits for timestampBits, workerIdBits, sequenceBits
     */
    public BusinessBitsAllocator(int timestampBits, int sourceBits, int typeBits, int extBits, int workerIdBits, int sequenceBits) {
        // make sure allocated 64 bits
        int allocateTotalBits = signBits + timestampBits + sourceBits + typeBits + extBits + workerIdBits + sequenceBits;
        Assert.isTrue(allocateTotalBits == TOTAL_BITS, "allocate not enough 64 bits");

        // initialize bits
        this.timestampBits = timestampBits;
        this.sourceBits = sourceBits;
        this.typeBits = typeBits;
        this.extBits = extBits;
        this.workerIdBits = workerIdBits;
        this.sequenceBits = sequenceBits;

        // initialize max value
        this.maxDeltaSeconds = ~(-1L << timestampBits);
        this.maxSource = ~(-1L << sourceBits);
        this.maxType = ~(-1L << typeBits);
        this.maxExt = ~(-1L << extBits);
        this.maxWorkerId = ~(-1L << workerIdBits);
        this.maxSequence = ~(-1L << sequenceBits);

        // initialize shift
        this.workerIdShift = sequenceBits;
        this.extShift = workerIdBits + workerIdShift;
        this.typeShift = extBits + extShift;
        this.sourceShift = typeBits + typeShift;
        this.timestampShift = sourceBits + sourceShift;

    }

    /**
     * Allocate bits for UID according to delta seconds & workerId & sequence<br>
     * <b>Note that: </b>The highest bit will always be 0 for sign
     *
     * @param deltaSeconds
     * @param workerId
     * @param sequence
     * @return
     */
    public long allocate(long deltaSeconds, long source, long type, long ext, long workerId, long sequence) {
        return (deltaSeconds << timestampShift) |
                (source << sourceShift) |
                (type << typeShift) |
                (ext << extShift) |
                (workerId << workerIdShift) |
                sequence;
    }

    /**
     * Getters
     */
    public int getSignBits() {
        return signBits;
    }

    public int getTimestampBits() {
        return timestampBits;
    }

    public int getSourceBits() {
        return sourceBits;
    }

    public int getTypeBits() {
        return typeBits;
    }

    public int getExtBits() {
        return extBits;
    }

    public int getWorkerIdBits() {
        return workerIdBits;
    }

    public int getSequenceBits() {
        return sequenceBits;
    }

    public long getMaxDeltaSeconds() {
        return maxDeltaSeconds;
    }

    public long getMaxSource() {
        return maxSource;
    }

    public long getMaxType() {
        return maxType;
    }

    public long getMaxExt() {
        return maxExt;
    }

    public long getMaxWorkerId() {
        return maxWorkerId;
    }

    public long getMaxSequence() {
        return maxSequence;
    }

    public int getTimestampShift() {
        return timestampShift;
    }

    public int getSourceShift() {
        return sourceShift;
    }

    public int getTypeShift() {
        return typeShift;
    }

    public int getExtShift() {
        return extShift;
    }

    public int getWorkerIdShift() {
        return workerIdShift;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
