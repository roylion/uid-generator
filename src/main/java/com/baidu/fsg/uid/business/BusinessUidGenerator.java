/*
 * Copyright (c) 2017 Baidu, Inc. All Rights Reserve.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.baidu.fsg.uid.business;

import com.baidu.fsg.uid.BitsAllocator;
import com.baidu.fsg.uid.UidGenerator;
import com.baidu.fsg.uid.exception.UidGenerateException;
import com.baidu.fsg.uid.utils.DateUtils;
import com.baidu.fsg.uid.worker.FixedWorkerIdAssigner;
import com.baidu.fsg.uid.worker.WorkerIdAssigner;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class BusinessUidGenerator implements UidGenerator, InitializingBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessUidGenerator.class);

    /**
     * Bits allocate
     */
    protected int timeBits = 30;
    protected int sourceBits = 10;
    protected int typeBits = 5;
    protected int extBits = 3;
    protected int workerBits = 4;
    protected int seqBits = 11;

    /**
     * Customer epoch, unit as second. For example 2020-05-12 (ms: 1589212800000)
     */
    protected String epochStr = "2020-05-12";
    protected long epochSeconds = TimeUnit.MILLISECONDS.toSeconds(1589212800000L);

    /**
     * Stable fields after spring bean initializing
     */
    protected BusinessBitsAllocator bitsAllocator;
    protected long workerId;

    /**
     * Volatile fields caused by nextId()
     */
    protected long sequence = 0L;
    protected long lastSecond = -1L;

    /**
     * Spring property
     */
    protected WorkerIdAssigner workerIdAssigner;

    @Override
    public void afterPropertiesSet() throws Exception {
        // initialize bits allocator
        bitsAllocator = new BusinessBitsAllocator(timeBits, sourceBits, typeBits, extBits, workerBits, seqBits);

        // initialize worker id
        workerId = workerIdAssigner.assignWorkerId();
        if (workerId > bitsAllocator.getMaxWorkerId()) {
            throw new RuntimeException("Worker id " + workerId + " exceeds the max " + bitsAllocator.getMaxWorkerId());
        }

        LOGGER.info("Initialized bits(1, {}, {}, {}, {}, {}, {}) for workerID:{}", timeBits, sourceBits, typeBits, extBits, workerBits, seqBits, workerId);
    }

    @Override
    public long getUID() throws UidGenerateException {
        throw new UnsupportedOperationException();
    }

    public long getUID(long source, long type, long ext) throws UidGenerateException {
        // Check
        if (source > bitsAllocator.getMaxSource()) {
            throw new UidGenerateException("source bits is exhausted. Refusing UID generate. source: " + source + " exceeds the max " + bitsAllocator.getMaxSource());
        }
        if (type > bitsAllocator.getMaxType()) {
            throw new UidGenerateException("type bits is exhausted. Refusing UID generate. type: " + type + " exceeds the max " + bitsAllocator.getMaxType());
        }
        if (ext > bitsAllocator.getMaxExt()) {
            throw new UidGenerateException("ext bits is exhausted. Refusing UID generate. ext: " + ext + " exceeds the max " + bitsAllocator.getMaxExt());
        }

        try {
            return nextId(source, type, ext);
        } catch (Exception e) {
            LOGGER.error("Generate unique id exception. ", e);
            throw new UidGenerateException(e);
        }
    }

    @Override
    public String parseUID(long uid) {
        long totalBits = BitsAllocator.TOTAL_BITS;
        long signBits = bitsAllocator.getSignBits();
        long timestampBits = bitsAllocator.getTimestampBits();
        long sourceBits = bitsAllocator.getSourceBits();
        long typeBits = bitsAllocator.getTypeBits();
        long extBits = bitsAllocator.getExtBits();
        long workerIdBits = bitsAllocator.getWorkerIdBits();
        long sequenceBits = bitsAllocator.getSequenceBits();

        // parse UID
        long sequence = (uid << (totalBits - sequenceBits)) >>> (totalBits - sequenceBits);
        long workerId = (uid << (totalBits - bitsAllocator.getExtShift())) >>> (totalBits - workerIdBits);
        long ext = (uid << (totalBits - bitsAllocator.getTypeShift())) >>> (totalBits - extBits);
        long type = (uid << (totalBits - bitsAllocator.getSourceShift())) >>> (totalBits - typeBits);
        long source = (uid << (totalBits - bitsAllocator.getTimestampShift())) >>> (totalBits - sourceBits);
        long deltaSeconds = uid >>> (totalBits - bitsAllocator.getTimestampShift());

        Date thatTime = new Date(TimeUnit.SECONDS.toMillis(epochSeconds + deltaSeconds));
        String thatTimeStr = DateUtils.formatByDateTimePattern(thatTime);

        // format as string
        return String.format("{\"UID\":\"%d\",\"timestamp\":\"%s\",\"source\":\"%d\",\"type\":\"%d\",\"ext\":\"%d\",\"workerId\":\"%d\",\"sequence\":\"%d\"}",
                uid, thatTimeStr, source, type, ext, workerId, sequence);
    }

    /**
     * Get UID
     *
     * @return UID
     * @throws UidGenerateException in the case: Clock moved backwards; Exceeds the max timestamp
     */
    protected synchronized long nextId(long source, long type, long ext) {
        long currentSecond = getCurrentSecond();

        // Clock moved backwards, refuse to generate uid
        if (currentSecond < lastSecond) {
            long refusedSeconds = lastSecond - currentSecond;
            throw new UidGenerateException("Clock moved backwards. Refusing for %d seconds", refusedSeconds);
        }

        // At the same second, increase sequence
        if (currentSecond == lastSecond) {
            sequence = (sequence + 1) & bitsAllocator.getMaxSequence();
            // Exceed the max sequence, we wait the next second to generate uid
            if (sequence == 0) {
                currentSecond = getNextSecond(lastSecond);
            }

            // At the different second, sequence restart from zero
        } else {
            sequence = 0L;
        }

        lastSecond = currentSecond;

        // Allocate bits for UID
        return bitsAllocator.allocate(currentSecond - epochSeconds, source, type, ext, workerId, sequence);
    }

    /**
     * Get next millisecond
     */
    private long getNextSecond(long lastTimestamp) {
        long timestamp = getCurrentSecond();
        while (timestamp <= lastTimestamp) {
            timestamp = getCurrentSecond();
        }

        return timestamp;
    }

    /**
     * Get current second
     */
    private long getCurrentSecond() {
        long currentSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        if (currentSecond - epochSeconds > bitsAllocator.getMaxDeltaSeconds()) {
            throw new UidGenerateException("Timestamp bits is exhausted. Refusing UID generate. Now: " + currentSecond);
        }

        return currentSecond;
    }

    /**
     * Setters for spring property
     */
    public void setWorkerIdAssigner(WorkerIdAssigner workerIdAssigner) {
        this.workerIdAssigner = workerIdAssigner;
    }

    public void setTimeBits(int timeBits) {
        if (timeBits > 0) {
            this.timeBits = timeBits;
        }
    }

    public void setWorkerBits(int workerBits) {
        if (workerBits > 0) {
            this.workerBits = workerBits;
        }
    }

    public void setSeqBits(int seqBits) {
        if (seqBits > 0) {
            this.seqBits = seqBits;
        }
    }

    public void setEpochStr(String epochStr) {
        if (StringUtils.isNotBlank(epochStr)) {
            this.epochStr = epochStr;
            this.epochSeconds = TimeUnit.MILLISECONDS.toSeconds(DateUtils.parseByDayPattern(epochStr).getTime());
        }
    }

    public static void main(String[] args) throws Exception {
        BusinessUidGenerator businessUidGenerator = new BusinessUidGenerator();
        businessUidGenerator.setWorkerIdAssigner(new FixedWorkerIdAssigner(15L));
        businessUidGenerator.afterPropertiesSet();
        long uid = businessUidGenerator.getUID(1023, 31, 7);
        System.out.println(uid);
        System.out.println(businessUidGenerator.parseUID(uid));
    }
}
