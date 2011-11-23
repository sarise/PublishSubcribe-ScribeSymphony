package p2p.system.peer.event;

import java.math.BigInteger;

import se.sics.kompics.Event;

public class SubscribePeer extends Event {

	private BigInteger topicID;
	
//-------------------------------------------------------------------
	public SubscribePeer(BigInteger topicID) {
		this.topicID = topicID;
	}
	
	public BigInteger getTopicID() {
		return this.topicID;
	}

}
