package com.baidu.fsg.uid.worker;

import com.baidu.fsg.uid.business.BusinessBitsAllocator;
import com.baidu.fsg.uid.exception.UidGenerateException;
import com.baidu.fsg.uid.utils.DateUtils;
import com.baidu.fsg.uid.utils.NamingThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * workerId分配策略：
 * 1 0 - maxWorkerId，每天递增分配；同时保证分配的workerId不会与其他已运行节点冲突。
 *
 * @author liugx
 */
public class NextDayRefreshWorkerIdAssigner implements WorkerIdAssigner, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(NextDayRefreshWorkerIdAssigner.class);

    private static final String SCHEDULE_NAME = "exist_refresh_schedule";

    private static final String NEXT = "workerId:";
    private static final String EXIST = "workerId:exist:";
    private static final String EXIST_PATTERN = "workerId:exist:*";

    private static final long REFRESH_DELAY = 12;
    private static final long EXPIRE = (REFRESH_DELAY << 1) + 1;

    private ScheduledExecutorService expireRefreshSchedule;

    @Resource
    private RedisTemplate redisTemplate;

    private BusinessBitsAllocator bitsAllocator;

    private long workerId;
    private String existKey;

    @Override
    public void afterPropertiesSet() throws Exception {
        expireRefreshSchedule = Executors.newSingleThreadScheduledExecutor(new NamingThreadFactory(SCHEDULE_NAME));
    }

    @Override
    public long assignWorkerId() {

        // 校验是否存在可用的workerId
        checkUseable();

        long nextWorkerId;
        do {
            nextWorkerId = nextWorkerId();
            if (nextWorkerId > bitsAllocator.getMaxWorkerId()) {
                // 超过最大值
                throw new RuntimeException("Worker id " + nextWorkerId + " exceeds the max " + bitsAllocator.getMaxWorkerId());
            }
        }
        while (!cacheWorkerId(nextWorkerId));

        return workerId;
    }

    /**
     * 递增获取下一次workerId
     *
     * @return
     */
    public long nextWorkerId() {

        String key = NEXT + DateUtils.getCurrentDayByDaySimplePattern();
        Long nextWorkerId = redisTemplate.opsForValue().increment(key, 1L) - 1;
        if (0 == nextWorkerId) {
            redisTemplate.expire(key, 1, TimeUnit.DAYS);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("get nextWorkerId. {}", nextWorkerId);
        }
        return nextWorkerId;
    }

    /**
     * 缓存当前节点，已存在则缓存失败
     *
     * @param workerId
     * @return
     */
    public boolean cacheWorkerId(long workerId) {
        String key = EXIST + workerId;
        // TODO 可能会存在bug导致缓存无法被清空
        if (redisTemplate.opsForValue().setIfAbsent(key, workerId)) {
            // 心跳机制，定时刷新缓存过期时间
            this.workerId = workerId;
            this.existKey = key;
            asyncRefresh();
            return true;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("workerId exist. {}", workerId);
        }

        return false;
    }

    /**
     * 异步刷新缓存过期时间
     */
    public void asyncRefresh() {
        Runnable runnable = () -> {
            LOGGER.info("refresh expire. {}", existKey);
            redisTemplate.expire(existKey, EXPIRE, TimeUnit.MINUTES);
        };
        expireRefreshSchedule.scheduleWithFixedDelay(runnable, 0, REFRESH_DELAY, TimeUnit.MINUTES);
    }

    /**
     * 校验是否启动节点已达上限
     */
    public void checkUseable() {
        Set keys = redisTemplate.keys(EXIST_PATTERN);
        if (null != keys && keys.size() > bitsAllocator.getMaxWorkerId()) {
            throw new UidGenerateException("There is no useable WorkerId!");
        }
    }

    public void setRedisTemplate(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void setBitsAllocator(BusinessBitsAllocator bitsAllocator) {
        this.bitsAllocator = bitsAllocator;
    }

}
