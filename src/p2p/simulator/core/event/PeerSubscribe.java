package p2p.simulator.core.event;

import java.math.BigInteger;

import se.sics.kompics.Event;

public final class PeerSubscribe extends Event {
	private final BigInteger peerId;

//-------------------------------------------------------------------	
	public PeerSubscribe(BigInteger peerId) {
		this.peerId = peerId;
	}
	
	//-------------------------------------------------------------------	
	public BigInteger getPeerId() {
		return peerId;
	}
}
