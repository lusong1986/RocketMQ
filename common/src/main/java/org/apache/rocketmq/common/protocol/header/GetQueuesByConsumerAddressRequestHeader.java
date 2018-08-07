package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 
 * 
 * @author lusong
 *
 */
public class GetQueuesByConsumerAddressRequestHeader implements CommandCustomHeader {
	@CFNotNull
	private String consumerAddress;

	public String getConsumerAddress() {
		return consumerAddress;
	}

	public void setConsumerAddress(String consumerAddress) {
		this.consumerAddress = consumerAddress;
	}

	@Override
	public void checkFields() throws RemotingCommandException {
	}

}
