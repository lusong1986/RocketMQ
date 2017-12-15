/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerAddressRecorder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.header.GetQueuesByConsumerAddressRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetQueuesByConsumerAddressResponseHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientManageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
	private final ScheduledExecutorService consumerClientIdsScheduledExecutorService = Executors
			.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
					"ClientManageProcessorConsumerClientIdScheduledThread"));

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        
		this.consumerClientIdsScheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					log.info("clearing getConsumerAddressQueueMap......");
					ConsumerAddressRecorder.getConsumerAddressQueueMap().clear();
				} catch (Exception e) {
					log.error("schedule consumer client ids error.", e);
				}
			}
		}, 60, 120, TimeUnit.SECONDS);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request);
    		case RequestCode.GET_QUEUES_BY_CONSUMER_ADDRESS:
    			return this.getQueuesByConsumerAddress(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
    

	public RemotingCommand getQueuesByConsumerAddress(ChannelHandlerContext ctx, RemotingCommand request)
			throws RemotingCommandException {
		final RemotingCommand response = RemotingCommand
				.createResponseCommand(GetQueuesByConsumerAddressResponseHeader.class);
		final GetQueuesByConsumerAddressRequestHeader requestHeader = (GetQueuesByConsumerAddressRequestHeader) request
				.decodeCommandCustomHeader(GetQueuesByConsumerAddressRequestHeader.class);

		final ConcurrentHashMap<String, String> queueSet = ConsumerAddressRecorder.getConsumerAddressQueueMap().get(
				requestHeader.getConsumerAddress());
		if (queueSet != null && !queueSet.isEmpty()) {
			try {
				String topicQueues = "";
				for (String topicQueue : queueSet.keySet()) {
					topicQueues += topicQueue + ",";
				}
				log.info(">>>>>>>>>>>getQueuesByConsumerAddress, consumer address:"
						+ requestHeader.getConsumerAddress() + ", topicQueues:" + topicQueues);
				response.setBody(topicQueues.getBytes("UTF-8"));
			} catch (UnsupportedEncodingException e) {
				log.error("encoding exception:" + e.getMessage());
				throw new RemotingCommandException("getQueuesByConsumerAddress encoding exception.");
			}
			response.setCode(ResponseCode.SUCCESS);
			response.setRemark(null);
			return response;
		}

		response.setCode(ResponseCode.SYSTEM_ERROR);
		response.setRemark("no consumer for this group, " + requestHeader.getConsumerAddress());
		return response;
	}
    

    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            heartbeatData.getClientID(),
            request.getLanguage(),
            request.getVersion()
        );

        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                    data.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;
            if (null != subscriptionGroupConfig) {
                isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                    newTopic,
                    subscriptionGroupConfig.getRetryQueueNums(),
                    PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            boolean changed = this.brokerController.getConsumerManager().registerConsumer(
                data.getGroupName(),
                clientChannelInfo,
                data.getConsumeType(),
                data.getMessageModel(),
                data.getConsumeFromWhere(),
                data.getSubscriptionDataSet(),
                isNotifyConsumerIdsChangedEnable
            );

            if (changed) {
                log.info("registerConsumer info changed {} {}",
                    data.toString(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                );
            }
        }

        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(),
                clientChannelInfo);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader requestHeader =
            (UnregisterClientRequestHeader) request
                .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
            ctx.channel(),
            requestHeader.getClientID(),
            request.getLanguage(),
            request.getVersion());
        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CheckClientRequestBody requestBody = CheckClientRequestBody.decode(request.getBody(),
            CheckClientRequestBody.class);

        if (requestBody != null && requestBody.getSubscriptionData() != null) {
            SubscriptionData subscriptionData = requestBody.getSubscriptionData();

            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return response;
            }

            try {
                FilterFactory.INSTANCE.get(subscriptionData.getExpressionType()).compile(subscriptionData.getSubString());
            } catch (Exception e) {
                log.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}",
                    requestBody.getClientId(), requestBody.getGroup(), requestBody.getSubscriptionData(), e.getMessage());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark(e.getMessage());
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
