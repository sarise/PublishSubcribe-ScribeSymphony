package p2p.simulator.core.event;

import se.sics.kompics.Event;

public final class AllPeerSubscribe_T extends Event {

//-------------------------------------------------------------------
	private String type; // "correlated", "twitter"
	
	public AllPeerSubscribe_T() {
		this.type = "twitter";
	}
	
	public String getType() {
		return type;
	}
	//-------------------------------------------------------------------	
	
}
