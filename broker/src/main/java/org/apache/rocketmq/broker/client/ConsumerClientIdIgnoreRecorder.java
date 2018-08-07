package org.apache.rocketmq.broker.client;

import java.util.concurrent.ConcurrentHashMap;

public class ConsumerClientIdIgnoreRecorder {

	private static ConcurrentHashMap<String/* Consumer Group */, String/* ignore consumer clientids */> ignoreConsumerClientIdsTable = new ConcurrentHashMap<String, String>(
			256);

	public static ConcurrentHashMap<String, String> getClientIdFilterMap() {
		return ignoreConsumerClientIdsTable;
	}

}
