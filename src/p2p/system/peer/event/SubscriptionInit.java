package p2p.system.peer.event;

import java.math.BigInteger;
import java.util.Set;
import java.util.Vector;

import p2p.system.peer.PeerAddress;


import se.sics.kompics.Event;

public class SubscriptionInit extends Event {
	
	private final Set<BigInteger> topicIDs;

//-------------------------------------------------------------------
	public SubscriptionInit(Set<BigInteger> topicIDs) {
		this.topicIDs = topicIDs;
	}
	
	public Set<BigInteger> getTopicIDs() {
		return this.topicIDs;
	}

}
