package se.sics.kompics.p2p.peer;

import java.math.BigInteger;

import p2p.system.peer.PeerAddress;


public class FindSucc extends PeerMessage {

	private static final long serialVersionUID = 1381490582457993230L;
	private final PeerAddress initiator;
	private final BigInteger id;
	private int fingerIndex;

//-------------------------------------------------------------------	
	public FindSucc(PeerAddress source, PeerAddress destination, PeerAddress initiator, BigInteger id, int fingerIndex) {
		super(source, destination);
		this.initiator = initiator;
		this.id = id;
		this.fingerIndex = fingerIndex;
	}

//-------------------------------------------------------------------	
	public PeerAddress getInitiator() {
		return this.initiator;
	}

//-------------------------------------------------------------------	
	public BigInteger getID() {
		return this.id;
	}

//-------------------------------------------------------------------	
	public int getFingerIndex() {
		return this.fingerIndex;
	}
}

