package se.sics.kompics.p2p.peer;

import p2p.system.peer.PeerAddress;

public class FindSuccReply extends PeerMessage {

	private static final long serialVersionUID = 1381490582457993230L;
	private final PeerAddress responsible;
	private int fingerIndex;

//-------------------------------------------------------------------	
	public FindSuccReply(PeerAddress source, PeerAddress destination, PeerAddress responsible, int fingerIndex) {
		super(source, destination);
		this.responsible = responsible;
		this.fingerIndex = fingerIndex;
	}

//-------------------------------------------------------------------	
	public PeerAddress getResponsible() {
		return this.responsible;
	}

//-------------------------------------------------------------------	
	public int getFingerIndex() {
		return this.fingerIndex;
	}
}

