## Apache RocketMQ [![Build Status](https://travis-ci.org/apache/rocketmq.svg?branch=master)](https://travis-ci.org/apache/rocketmq) [![Coverage Status](https://coveralls.io/repos/github/apache/rocketmq/badge.svg?branch=master)](https://coveralls.io/github/apache/rocketmq?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-all/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Corg.apache.rocketmq)
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://rocketmq.apache.org/dowloading/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

**主要新增功能点：

**1.slave上接入mongodb副本集存储，方便消息统计、查询，结合[apache-rocketmq-console-ext]（https://github.com/lusong1986/apache-rocketmq-console-ext ） 使用**

slave上配置mongodb例子,修改slave的broker配置文件 broker-b.properties加如下配置：
* mongoRepSetHosts=127.0.0.1:28017,127.0.0.1:28087,127.0.0.1:28019
* mongoDbName=mq_messages
* mongoUser=user
* mongoPassword=password

**2.broker端支持灰度下线、上线消费者，并提供admin api**

**3.broker端支持查询生产组列表信息，并提供admin api**

**4.broker端支持查询每个消费组的消费数量统计，并提供admin api**

**5.broker端支持查询每个消费者绑定的queue列表，并提供admin api**

**6.消费客户端rebalance默认算法改为一致性hash算法**

----------

## Learn it & Contact us
* Mailing Lists: <https://rocketmq.apache.org/about/contact/>
* Home: <https://rocketmq.apache.org>
* Docs: <https://rocketmq.apache.org/docs/quick-start/>
* Issues: <https://issues.apache.org/jira/browse/RocketMQ>
* Ask: <https://stackoverflow.com/questions/tagged/rocketmq>
 

----------

## Apache RocketMQ Community
* [RocketMQ Community Projects](https://github.com/apache/rocketmq-externals)


----------
## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
