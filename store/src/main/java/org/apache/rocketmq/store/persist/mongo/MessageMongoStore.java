package org.apache.rocketmq.store.persist.mongo;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.persist.MsgStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

public class MessageMongoStore implements MsgStore {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private MessageMongoStoreConfig messageMongoStoreConfig;

	private MongoClient mgClient;

	private DB mqDb;

	private AtomicLong totalRecordsValue = new AtomicLong(0);

	private final SimpleDateFormat YYYYMMDDFMT = new SimpleDateFormat("yyyyMMdd");

	private static final String COLLECTION_PREFIX = "messages_";

	public MessageMongoStore(MessageMongoStoreConfig messageMongoStoreConfig) {
		this.messageMongoStoreConfig = messageMongoStoreConfig;
	}

	@SuppressWarnings("deprecation")
	public boolean open() {
		if (null == messageMongoStoreConfig.getMongoRepSetHosts()
				|| messageMongoStoreConfig.getMongoRepSetHosts().isEmpty()) {
			return false;
		}

		if (null == messageMongoStoreConfig.getMongoDbName() || messageMongoStoreConfig.getMongoDbName().isEmpty()) {
			return false;
		}

		try {
			List<ServerAddress> addresses = new ArrayList<ServerAddress>();
			String[] mongoHosts = messageMongoStoreConfig.getMongoRepSetHosts().trim().split(",");
			if (mongoHosts != null && mongoHosts.length > 0) {
				for (String mongoHost : mongoHosts) {
					if (mongoHost != null && mongoHost.length() > 0) {
						String[] mongoServer = mongoHost.split(":");
						if (mongoServer != null && mongoServer.length == 2) {
							log.info("add mongo server>>" + mongoServer[0] + ":" + mongoServer[1]);
							ServerAddress address = new ServerAddress(mongoServer[0].trim(),
									Integer.parseInt(mongoServer[1].trim()));
							addresses.add(address);
						}
					}
				}
			}

			List<MongoCredential> credentialsList = new LinkedList<MongoCredential>();
			MongoCredential credential = MongoCredential.createCredential(messageMongoStoreConfig.getMongoUser(),
					messageMongoStoreConfig.getMongoDbName(), messageMongoStoreConfig.getMongoPassword().toCharArray());
			credentialsList.add(credential);
			mgClient = new MongoClient(addresses, credentialsList);
			mqDb = mgClient.getDB(messageMongoStoreConfig.getMongoDbName());

			log.info("open mongodb successfully.");
			return true;
		} catch (Throwable e) {
			log.error("open mongo Exeption " + e.getMessage(), e);
		}

		return false;
	}

	public void close() {
		try {
			if (this.mgClient != null) {
				this.mgClient.close();
			}
		} catch (Throwable e) {
			log.error("close mongo Exeption", e);
		}
	}

	private static Date lastday(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(date.getTime());
		cal.add(Calendar.DAY_OF_MONTH, -1);

		return cal.getTime();
	}

	@Override
	public boolean store(List<MessageExt> msgs) {
		if (null == msgs || msgs.size() == 0) {
			log.warn("store msgs is empty.");
			return false;
		}

		if (null == this.mqDb) {
			log.warn("mongo mqDb is null.");
			return false;
		}

		try {
			for (MessageExt messageExt : msgs) {
				DBCollection mqMessageCollection = mqDb.getCollection(
						COLLECTION_PREFIX + YYYYMMDDFMT.format(new Date(messageExt.getStoreTimestamp())));

				final MongoMessage mongoMessage = generateMongoMessage(messageExt);

				try {
					DBObject dbObject = BasicDBObjectUtils.castModel2DBObject(mongoMessage);
					mqMessageCollection.insert(dbObject, WriteConcern.MAJORITY);

					this.totalRecordsValue.addAndGet(1);

					// 以下是处理预发的事务消息，更新状态tranStatus
					if (messageExt.getPreparedTransactionOffset() > 0) {
						final int tranType = MessageSysFlag.getTransactionValue(messageExt.getSysFlag());
						int tranStatus = 0;
						if (tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
							tranStatus = 1;// commit
						} else if (tranType == MessageSysFlag.TRANSACTION_ROLLBACK_TYPE) {
							tranStatus = 2;// rollback
						}

						if (tranStatus > 0) {
							final DBObject query = new BasicDBObject();
							query.put("commitLogOffset", messageExt.getPreparedTransactionOffset());
							DBObject retObject = mqMessageCollection.findOne(query);
							if (null == retObject) {
								final String lastDay = YYYYMMDDFMT
										.format(lastday(new Date(messageExt.getStoreTimestamp())));
								mqMessageCollection = mqDb.getCollection(COLLECTION_PREFIX + lastDay);
								if (mqMessageCollection != null) {
									retObject = mqMessageCollection.findOne(query);
									// log.info("query prepare message again in table messages_" + lastDay);
								}
							}

							if (retObject != null) {
								retObject.put("tranStatus", tranStatus);
								mqMessageCollection.update(query, retObject);
								// log.info("update prepare message tranStatus: " + retObject);
							}
						}
					}

				} catch (Throwable e) {
					log.warn("insert mongo error:" + e.getMessage(), e);
				}
			}

			return true;
		} catch (Throwable e) {
			log.warn("mongo store messageExt Exception", e);
		}

		return false;
	}

	private MongoMessage generateMongoMessage(MessageExt messageExt) {
		final MongoMessage mongoMessage = new MongoMessage();
		mongoMessage.setQueueId(messageExt.getQueueId());
		mongoMessage.setStoreSize(messageExt.getStoreSize());
		mongoMessage.setQueueOffset(messageExt.getQueueOffset());
		mongoMessage.setSysFlag(messageExt.getSysFlag());
		mongoMessage.setStoreTime(new Date(messageExt.getStoreTimestamp()));
		mongoMessage.setBornTime(new Date(messageExt.getBornTimestamp()));
		mongoMessage.setBornHost(getHostString(messageExt.getBornHost()));
		mongoMessage.setStoreHost(getHostString(messageExt.getStoreHost()));

		mongoMessage.setMsgId(messageExt.getMsgId());
		mongoMessage.setCommitLogOffset(messageExt.getCommitLogOffset());
		mongoMessage.setBodyCRC(messageExt.getBodyCRC());
		mongoMessage.setReconsumeTimes(messageExt.getReconsumeTimes());
		mongoMessage.setPreparedTransactionOffset(messageExt.getPreparedTransactionOffset());
		mongoMessage.setTopic(messageExt.getTopic());
		mongoMessage.setFlag(messageExt.getFlag());
		mongoMessage.setTags(messageExt.getTags() == null ? "" : messageExt.getTags());
		mongoMessage.setKeys(messageExt.getKeys() == null ? "" : messageExt.getKeys());

		mongoMessage.setCreateTime(new Date());
		mongoMessage.setUpdateTime(mongoMessage.getCreateTime());

		String bodyContentStr = "";
		try {
			bodyContentStr = new String(messageExt.getBody(), "utf-8");
		} catch (Throwable e) {
			log.warn("failed to convert text-based Message content:{}" + e.getMessage(), messageExt.getMsgId());
		}
		mongoMessage.setContent(bodyContentStr);

		mongoMessage.setPropertiesString(JSONObject.parseObject(JSON.toJSONString(messageExt.getProperties())));

		return mongoMessage;
	}

	private String getHostString(SocketAddress host) {
		if (host != null) {
			InetSocketAddress inetSocketAddress = (InetSocketAddress) host;
			return inetSocketAddress.getAddress().getHostAddress() + ":" + inetSocketAddress.getPort();
		}

		return "";
	}

	public MessageMongoStoreConfig getMessageMongoStoreConfig() {
		return messageMongoStoreConfig;
	}

	public void setMessageMongoStoreConfig(MessageMongoStoreConfig messageMongoStoreConfig) {
		this.messageMongoStoreConfig = messageMongoStoreConfig;
	}

}
