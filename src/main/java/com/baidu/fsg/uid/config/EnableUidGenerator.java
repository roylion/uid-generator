package com.baidu.fsg.uid.config;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.stereotype.Repository;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@MapperScan(basePackages = "com.baidu.fsg.uid",
        annotationClass = Repository.class)
public @interface EnableUidGenerator {
}
