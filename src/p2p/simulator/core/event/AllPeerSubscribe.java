package p2p.simulator.core.event;

import se.sics.kompics.Event;

public final class AllPeerSubscribe extends Event {

//-------------------------------------------------------------------
	private String type; // "correlated", "twitter"
	
	public AllPeerSubscribe(String type) {
		this.type = type;
	}
	
	public String getType() {
		return type;
	}
	//-------------------------------------------------------------------	
	
}
