package org.apache.rocketmq.store.persist;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.persist.mongo.MessageMongoStore;
import org.apache.rocketmq.store.persist.mongo.MessageMongoStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消息持久化服务
 * 
 * @author lusong
 * @since 2016-06-01
 */
public class MsgPersistService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private static final String MONGO = "mongo";

	private MsgStore msgStore;

	private volatile boolean opened = false;

	private LinkedBlockingQueue<MessageExt> msgQueue = new LinkedBlockingQueue<MessageExt>(100000);

	public MsgPersistService(final DefaultMessageStore store) {
		log.info("Start to init msgStore in MsgPersistService Constrcutor.");
		MessageStoreConfig messageStoreConfig = store.getMessageStoreConfig();
		if (messageStoreConfig != null) {
			if (MONGO.equals(messageStoreConfig.getMsgStoreType())) {
				MessageMongoStoreConfig messageMongoStoreConfig = new MessageMongoStoreConfig();
				messageMongoStoreConfig.setMongoDbName(messageStoreConfig.getMongoDbName());
				messageMongoStoreConfig.setMongoRepSetHosts(messageStoreConfig.getMongoRepSetHosts());
				messageMongoStoreConfig.setMongoUser(messageStoreConfig.getMongoUser());
				messageMongoStoreConfig.setMongoPassword(messageStoreConfig.getMongoPassword());

				this.msgStore = new MessageMongoStore(messageMongoStoreConfig);
			}

			if (msgStore != null) {
				opened = msgStore.open();
			}
		}

		if (opened) {
			log.info(">>>>>>>>>>>get msgStore connection correctly.");
		} else {
			log.warn(">>>>>>>>>>>get msgStore connection wrong.");
		}
	}

	/**
	 * 向队列中添加message，队列满情况下，丢弃请求
	 */
	public void putMessage(final MessageExt msg) {
		if (!opened) {
			return;
		}

		boolean offer = this.msgQueue.offer(msg);
		if (!offer) {
			if (log.isDebugEnabled()) {
				log.debug("putMessage msg failed, {}", msg);
			}
		}
	}

	@Override
	public void run() {
		log.info(this.getServiceName() + " service started");

		while (!this.isStopped()) {
			try {
				MessageExt msg = this.msgQueue.poll(3000, TimeUnit.MILLISECONDS);
				if (msg != null) {
					log.info("Start to persist msg:" + msg);

					this.persist(msg);
				}
			} catch (Throwable e) {
				log.warn(this.getServiceName() + " service has exception. ", e);
			}
		}

		log.info(this.getServiceName() + " service end");
	}

	private boolean persist(MessageExt msg) {
		List<MessageExt> msgs = new ArrayList<MessageExt>();
		msgs.add(msg);
		msgStore.store(msgs);

		return false;
	}

	@Override
	public String getServiceName() {
		return MsgPersistService.class.getSimpleName();
	}
}
