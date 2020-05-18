package com.baidu.fsg.uid.config;

import com.baidu.fsg.uid.business.BusinessUidGenerator;
import com.baidu.fsg.uid.worker.WorkerIdAssigner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration
public class UidGeneratorConfig {

    /**
     * 高位序列，影响每天的uid生成总量
     */
    @Value("${uid.generator.hignSeqBits:4}")
    private int hignSeqBits;

    /**
     * 工作节点， 影响系统的重启次数
     */
    @Value("${uid.generator.workerIdBits:2}")
    private int workerIdBits;

    /**
     * 系统标识
     */
    @Value("${uid.generator.systemIdBits:4}")
    private int systemIdBits;

    /**
     * 系统业务标识
     */
    @Value("${uid.generator.bizIdBits:2}")
    private int bizIdBits;

    /**
     * 扩展
     */
    @Value("${uid.generator.extraInfoBits:2}")
    private int extraInfoBits;

    /**
     * 分库
     */
    @Value("${uid.generator.hignSeqBits:3}")
    private int shardingBits;

    /**
     * 序列节点， 影响每天的uid生成总量
     */
    @Value("${uid.generator.seqBits:4}")
    private int seqBits;

    @Bean
    @Lazy(false)
    public BusinessUidGenerator businessUidGenerator(WorkerIdAssigner workerIdAssigner) {
        BusinessUidGenerator businessUidGenerator = new BusinessUidGenerator();
        businessUidGenerator.setWorkerIdAssigner(workerIdAssigner);
        businessUidGenerator.setHighSeqBits(hignSeqBits);
        businessUidGenerator.setWorkerIdBits(workerIdBits);
        businessUidGenerator.setSystemIdBits(systemIdBits);
        businessUidGenerator.setBizIdBits(bizIdBits);
        businessUidGenerator.setExtraInfoBits(extraInfoBits);
        businessUidGenerator.setShardingBits(shardingBits);
        businessUidGenerator.setSequenceBits(seqBits);
        return businessUidGenerator;
    }

}
