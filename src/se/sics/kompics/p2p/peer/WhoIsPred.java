package se.sics.kompics.p2p.peer;

import p2p.system.peer.PeerAddress;

public class WhoIsPred extends PeerMessage {

	private static final long serialVersionUID = 1381490582457993230L;

//-------------------------------------------------------------------	
	public WhoIsPred(PeerAddress source, PeerAddress destination) {
		super(source, destination);
	}
}

