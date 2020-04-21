package com.baidu.fsg.uid.config;

import com.baidu.fsg.uid.impl.DefaultUidGenerator;
import com.baidu.fsg.uid.worker.DisposableWorkerIdAssigner;
import com.baidu.fsg.uid.worker.WorkerIdAssigner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

@Configuration
public class DefaultUidGeneratorConfig {

    /**
     * 当前与epoch时间差(单位秒) 占用比特，影响系统使用的总时间
     */
    @Value("${uid.generator.timeBits:29}")
    private int timeBits;

    /**
     * 工作节点， 影响系统的重启次数
     */
    @Value("${uid.generator.workerBits:21}")
    private int workerBits;

    /**
     * 序列节点， 影响单节点的并发次数 qps
     */
    @Value("${uid.generator.seqBits:13}")
    private int seqBits;

    /**
     * 初始时间节点， 一定要改成第一次启动时的时间，不然会浪费好几年的运行时间
     */
    @Value("${uid.generator.epochStr:2020-04-21}")
    private String epochStr;

    @Bean
    @Lazy(false)
    public DefaultUidGenerator defaultUidGenerator() {
        DefaultUidGenerator defaultUidGenerator = new DefaultUidGenerator();
        defaultUidGenerator.setWorkerIdAssigner(disposableWorkerIdAssigner());
        defaultUidGenerator.setTimeBits(timeBits);
        defaultUidGenerator.setWorkerBits(workerBits);
        defaultUidGenerator.setSeqBits(seqBits);
        defaultUidGenerator.setEpochStr(epochStr);
        return defaultUidGenerator;
    }

    @Bean
    public WorkerIdAssigner disposableWorkerIdAssigner() {
        return new DisposableWorkerIdAssigner();
    }
}
