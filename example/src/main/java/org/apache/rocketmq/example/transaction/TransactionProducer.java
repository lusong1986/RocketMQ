package org.apache.rocketmq.example.transaction;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class TransactionProducer {

	public static void main(String[] args) throws MQClientException, InterruptedException {
		TransactionListener transactionListener = new TransactionListenerImpl();
		TransactionMQProducer producer = new TransactionMQProducer("lusongTranscationProducerGroup");
		ExecutorService executorService = new ThreadPoolExecutor(4, 5, 100, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(2000), new ThreadFactoryImpl("client-transaction-msg-check-thread"));

		producer.setNamesrvAddr("192.168.56.101:9876");
		producer.setExecutorService(executorService);
		producer.setTransactionListener(transactionListener);
		producer.start();

		String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
		for (int i = 0; i < 3; i++) {
			try {
				Message msg = new Message("TranscationTopicTest", tags[i % tags.length], "KEY" + i,
						("Hello RocketMQ Transcation " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
				TransactionSendResult sendResult = producer.sendMessageInTransaction(msg, null);
				System.out.printf("%s%n", ToStringBuilder.reflectionToString(sendResult));

				Thread.sleep(10);
			} catch (MQClientException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}

		for (int i = 0; i < 100000; i++) {
			Thread.sleep(1000);
		}
		producer.shutdown();
	}
}
