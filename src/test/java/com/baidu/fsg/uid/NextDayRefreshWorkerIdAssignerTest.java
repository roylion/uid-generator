package com.baidu.fsg.uid;

import com.baidu.fsg.uid.business.BusinessBitsAllocator;
import com.baidu.fsg.uid.config.RedisConfig;
import com.baidu.fsg.uid.worker.NextDayRefreshWorkerIdAssigner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RedisConfig.class)
public class NextDayRefreshWorkerIdAssignerTest {


    @Resource
    private NextDayRefreshWorkerIdAssigner nextDayRefreshWorkerIdAssigner;

    @Test
    public void ttt() throws IOException {
        nextDayRefreshWorkerIdAssigner.setBitsAllocator(new BusinessBitsAllocator(6, 2, 4, 2, 2, 3, 2));

        System.out.println(nextDayRefreshWorkerIdAssigner.assignWorkerId());
    }
}
