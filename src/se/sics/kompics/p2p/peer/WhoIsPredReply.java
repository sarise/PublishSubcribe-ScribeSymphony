package se.sics.kompics.p2p.peer;

import p2p.system.peer.PeerAddress;

public class WhoIsPredReply extends PeerMessage {

	private static final long serialVersionUID = 1381490582457993230L;
	private final PeerAddress pred;
	private final PeerAddress[] succList;

//-------------------------------------------------------------------	
	public WhoIsPredReply(PeerAddress source, PeerAddress destination, PeerAddress pred, PeerAddress[] succList) {
		super(source, destination);
		this.pred = pred;
		this.succList = succList;
	}

//-------------------------------------------------------------------	
	public PeerAddress getPred() {
		return this.pred;
	}

//-------------------------------------------------------------------	
	public PeerAddress[] getSuccList() {
		return this.succList;
	}
}


