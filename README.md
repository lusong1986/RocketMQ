
**主要新增功能点(这个fork是建立在apache rocketmq4.3.0之上)：**

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
## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
