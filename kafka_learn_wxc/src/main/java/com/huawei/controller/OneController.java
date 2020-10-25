package com.huawei.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author:wxc
 * project: kafka-learn
 * ClassName: OneController
 * Date: 2020/10/25 22:04 周日
 * yy:猥琐别浪，等我发育。
 */
@RequestMapping
@RestController
public class OneController {

    @RequestMapping("index")
    public  String index(){
        return "hello,kafka";
    }
}
