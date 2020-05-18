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

import com.baidu.fsg.uid.exception.UidGenerateException;
import com.baidu.fsg.uid.utils.DateUtils;
import com.baidu.fsg.uid.worker.FixedWorkerIdAssigner;
import com.baidu.fsg.uid.worker.NextDayRefreshWorkerIdAssigner;
import com.baidu.fsg.uid.worker.WorkerIdAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class BusinessUidGenerator implements InitializingBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessUidGenerator.class);

    /**
     * Bits allocate
     */
    protected int highSeqBits = 6;
    protected int workerIdBits = 2;
    protected int appIdBits = 4;
    protected int bizTypeBits = 2;
    protected int extraTagBits = 2;
    protected int shardingIdBits = 3;
    protected int sequenceBits = 2;

    /**
     * Stable fields after spring bean initializing
     */
    protected BusinessBitsAllocator bitsAllocator;
    protected long workerId;

    /**
     * Volatile fields caused by nextId()
     */
    protected long highSeq = 0;
    protected long sequence = 0L;
    protected String date;
    protected long lastSecond = -1L;
    protected String lastDate = "-1";

    /**
     * Spring property
     */
    protected WorkerIdAssigner workerIdAssigner;

    @Override
    public void afterPropertiesSet() throws Exception {
        // initialize bits allocator
        bitsAllocator = new BusinessBitsAllocator(highSeqBits, workerIdBits, appIdBits, bizTypeBits, extraTagBits, shardingIdBits, sequenceBits);
        if (workerIdAssigner instanceof NextDayRefreshWorkerIdAssigner) {
            ((NextDayRefreshWorkerIdAssigner) workerIdAssigner).setBitsAllocator(bitsAllocator);
        }

        // initialize worker id
        workerId = workerIdAssigner.assignWorkerId();
        if (workerId > bitsAllocator.getMaxWorkerId()) {
            throw new RuntimeException("Worker id " + workerId + " exceeds the max " + bitsAllocator.getMaxWorkerId());
        }
        date = DateUtils.getCurrentDayByDaySimplePattern();

        LOGGER.info("Initialized bits(8, {}, {}, {}, {}, {}, {}, {}) for workerID:{}", highSeqBits, workerIdBits, appIdBits, bizTypeBits, extraTagBits, shardingIdBits, sequenceBits, workerId);
    }

    public String getUID(long appId, long bizType, long extraTag, long shardingId) throws UidGenerateException {
        // Check
        checkBusinessParams(appId, bizType, extraTag, shardingId);

        try {
            return nextId(appId, bizType, extraTag, shardingId);
        } catch (Exception e) {
            LOGGER.error("Generate unique id exception. ", e);
            throw new UidGenerateException(e);
        }
    }

    public String parseUID(String uid) {
        int highSeqBegin = bitsAllocator.getHighSeqBegin();
        int workerIdBegin = bitsAllocator.getWorkerIdBegin();
        int appIdBegin = bitsAllocator.getAppIdBegin();
        int bizTypeBegin = bitsAllocator.getBizTypeBegin();
        int extraTagBegin = bitsAllocator.getExtraTagBegin();
        int shardingIdBegin = bitsAllocator.getShardingIdBegin();
        int sequenceBegin = bitsAllocator.getSequenceBegin();

        String date = uid.substring(0, highSeqBegin);
        String highSeq = uid.substring(highSeqBegin, workerIdBegin);
        String workerId = uid.substring(workerIdBegin, appIdBegin);
        String appId = uid.substring(appIdBegin, bizTypeBegin);
        String bizType = uid.substring(bizTypeBegin, extraTagBegin);
        String extraTag = uid.substring(extraTagBegin, shardingIdBegin);
        String shardingId = uid.substring(shardingIdBegin, sequenceBegin);
        String sequence = uid.substring(sequenceBegin);

        // format as string
        return String.format("{\"uid\":\"%s\",\"date\":\"%s\",\"highSeq\":%s,\"workerId\":%s,\"appId\":%s,\"bizType\":%s,\"extraTag\":%s,\"shardingId\":%s,\"sequence\":%s}",
                uid, date, highSeq, workerId, appId, bizType, extraTag, shardingId, sequence);
    }

    /**
     * Get UID
     *
     * @return UID
     * @throws UidGenerateException in the case: Clock moved backwards; Exceeds the max timestamp
     */
    protected synchronized String nextId(long appId, long bizType, long extraTag, long shardingId) {

        String currentDate;
        long currentSecond;

        // Clock moved backwards, refuse to generate uid
        currentSecond = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
        if (currentSecond < lastSecond) {
            long refusedSeconds = lastSecond - currentSecond;
            throw new UidGenerateException("Clock moved backwards. Refusing for %d seconds", refusedSeconds);
        }

        // 次日 清空
        currentDate = DateUtils.getCurrentDayByDaySimplePattern();
        if (currentDate.compareTo(lastDate) > 0) {
            highSeq = 0;
            lastDate = currentDate;
        }

        // Exceed the max sequence, increase highSeq
        if (++sequence > bitsAllocator.getMaxSequence()) {
            sequence = 0L;
            // 自增highSeq
            if (++highSeq > bitsAllocator.getMaxHignSeq()) {
                throwUidGenerateException("HighSeq", highSeq, bitsAllocator.getMaxHignSeq());
            }
        }

        lastSecond = currentSecond;
        // Allocate bits for UID
        return bitsAllocator.allocate(currentDate, highSeq, workerId, appId, bizType, extraTag, shardingId, sequence);
    }

    protected void checkBusinessParams(long appId, long bizType, long extraTag, long shardingId) {
        if (appId > bitsAllocator.getMaxAppId()) {
            throwUidGenerateException("AppId", appId, bitsAllocator.getMaxAppId());
        }
        if (bizType > bitsAllocator.getMaxBizType()) {
            throwUidGenerateException("BizType", bizType, bitsAllocator.getMaxBizType());
        }
        if (extraTag > bitsAllocator.getMaxExtraTag()) {
            throwUidGenerateException("ExtraTag", extraTag, bitsAllocator.getMaxExtraTag());
        }
        if (shardingId > bitsAllocator.getMaxShardingId()) {
            throwUidGenerateException("ShardingId", shardingId, bitsAllocator.getMaxShardingId());
        }
    }

    private void throwUidGenerateException(String bitName, long bitVal, long maxBitVal) {
        throw new UidGenerateException(bitName + " bits is exhausted. Refusing UID generate. " + bitName + ":" + bitVal + " exceeds the max " + maxBitVal);
    }

    /**
     * Setters for spring property
     */
    public void setWorkerIdAssigner(WorkerIdAssigner workerIdAssigner) {
        this.workerIdAssigner = workerIdAssigner;
    }

    public void setHighSeqBits(int highSeqBits) {
        this.highSeqBits = highSeqBits;
    }

    public void setWorkerIdBits(int workerIdBits) {
        this.workerIdBits = workerIdBits;
    }

    public void setAppIdBits(int appIdBits) {
        this.appIdBits = appIdBits;
    }

    public void setBizTypeBits(int bizTypeBits) {
        this.bizTypeBits = bizTypeBits;
    }

    public void setExtraTagBits(int extraTagBits) {
        this.extraTagBits = extraTagBits;
    }

    public void setShardingIdBits(int shardingIdBits) {
        this.shardingIdBits = shardingIdBits;
    }

    public void setSequenceBits(int sequenceBits) {
        this.sequenceBits = sequenceBits;
    }

    public static void main(String[] args) throws Exception {
        BusinessUidGenerator businessUidGenerator = new BusinessUidGenerator();
        businessUidGenerator.setWorkerIdAssigner(new FixedWorkerIdAssigner(99));
        businessUidGenerator.afterPropertiesSet();

        int size = 999999;
        Set<String> ss = new TreeSet<String>();
        for (int i = 0; i < size; i++) {
            String uid = businessUidGenerator.getUID(9999, 99, 99, 999);
            if (!ss.add(uid)) {
                System.out.println("重复" + uid);
            }
        }

        System.out.println(ss.size() == size);
    }
}
