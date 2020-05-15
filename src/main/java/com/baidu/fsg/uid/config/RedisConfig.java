package com.baidu.fsg.uid.config;

import com.baidu.fsg.uid.worker.NextDayRefreshWorkerIdAssigner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
@PropertySource("classpath:/uid/redis.properties")
public class RedisConfig {


    @Bean
    public RedisTemplate redisTemplate() {
        JedisConnectionFactory confac = new JedisConnectionFactory();
        confac.setDatabase(0);
        confac.setHostName("localhost");
        confac.afterPropertiesSet();

        RedisTemplate<Object, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(confac);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new JdkSerializationRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new JdkSerializationRedisSerializer());
        return template;
    }

    @Bean
    public NextDayRefreshWorkerIdAssigner nextDayRefreshWorkerIdAssigner() {
        NextDayRefreshWorkerIdAssigner nextDayRefreshWorkerIdAssigner = new NextDayRefreshWorkerIdAssigner();
        nextDayRefreshWorkerIdAssigner.setRedisTemplate(redisTemplate());
        return nextDayRefreshWorkerIdAssigner;
    }
}
