package org.apache.rocketmq.store.persist;

import java.util.List;

import org.apache.rocketmq.common.message.MessageExt;

public interface MsgStore {

	public boolean open();

	public void close();

	public boolean store(final List<MessageExt> msgs);

}
