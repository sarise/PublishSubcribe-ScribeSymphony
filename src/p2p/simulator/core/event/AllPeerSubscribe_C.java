package p2p.simulator.core.event;

import se.sics.kompics.Event;

public final class AllPeerSubscribe_C extends Event {

//-------------------------------------------------------------------
	private String type; // "correlated", "twitter"
	
	public AllPeerSubscribe_C() {
		this.type = "correlated";
	}
	
	public String getType() {
		return type;
	}
	//-------------------------------------------------------------------	
	
}
