/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

	public static void main(String[] args) throws InterruptedException, MQClientException {

		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("lusong-consumer-group3");
		consumer.setNamesrvAddr("192.168.56.101:9876");
		consumer.setConsumeThreadMax(64);
		consumer.setConsumeThreadMin(20);
		consumer.setConsumeConcurrentlyMaxSpan(200);
		consumer.setPullBatchSize(32);
		consumer.setPullThresholdForQueue(500);

		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

		/*
		 * Subscribe one more more topics to consume.
		 */
		consumer.subscribe("LUSONG_TOPIC4", "*");

		/*
		 * Register callback to execute on arrival of messages fetched from brokers.
		 */
		consumer.registerMessageListener(new MessageListenerConcurrently() {

			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				Thread currentThread = Thread.currentThread();
				System.out.println(">>>>>>>>>>>>>>>>>>>>>" + currentThread.getName()
						+ ">>>>>>>>>>consumeMessage>>>>>>>>>>>>" + msgs.size());

				try {
					Map<String, String> properties = msgs.get(0).getProperties();
					for (Entry<String, String> entrySet : properties.entrySet()) {
						System.out.println("" + entrySet.getKey() + ">>>" + entrySet.getValue());
					}
					System.out.println(">>>>>>>>topic>>>" +msgs.get(0).getTopic());
					System.out.println(">>>>>>>>reconsumeTimes>>>" +msgs.get(0).getReconsumeTimes());
					
					try {
						ByteBuffer msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
						String messageId = MessageDecoder.createMessageId(msgIdMemory, msgs.get(0).getStoreHostBytes(),
								msgs.get(0).getCommitLogOffset());
						System.out.println(">>>>>>>>>>>>>>offsetmessageId :"+messageId);
					} catch (Exception e) {
						e.printStackTrace();
					}

					System.out.printf(">>>>>>>>>>>>>>>>>>" + currentThread.getName() + " Receive New Messages: "
							+ msgs.get(0).getStoreHost());

					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
					}

					System.out.printf(
							">>>>>>>>>>>>>>>>>>msgs.get(0).getDelayTimeLevel():" + msgs.get(0).getDelayTimeLevel());

					int i = 0;
					if (++i > 0) {
						if (msgs.get(0).getDelayTimeLevel() >= 2 + 3) {
							System.out.println(">>>>>>>>>>>>>>>>>set setDelayLevelWhenNextConsume to -1");
							context.setDelayLevelWhenNextConsume(-1);
						}

						System.out.println(">>>>>>>>>>>>>>>>>throw exception>>>>>>>>>>\n\n\n\n\n");
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}

					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				} catch (Exception e) {
					e.printStackTrace();
					throw e;
				}
			}
		});

		/*
		 * Launch the consumer instance.
		 */
		consumer.start();

		System.out.printf("Consumer Started.%n");
	}
}

// MessageId messageId = null;
// try {
// messageId = MessageDecoder.decodeMessageId(msgs.get(0).getMsgId());
// System.out.println(messageId.getOffset());
// } catch (Exception e) {
// e.printStackTrace();
// }
