package se.sics.kompics.p2p.peer;

import p2p.system.peer.PeerAddress;

public class Notify extends PeerMessage {

	private static final long serialVersionUID = 1381490582457993230L;
	private final PeerAddress id;

//-------------------------------------------------------------------	
	public Notify(PeerAddress source, PeerAddress destination, PeerAddress id) {
		super(source, destination);
		this.id = id;
	}

//-------------------------------------------------------------------	
	public PeerAddress getID() {
		return this.id;
	}
}

