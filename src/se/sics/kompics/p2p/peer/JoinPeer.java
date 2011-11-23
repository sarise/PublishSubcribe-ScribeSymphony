package se.sics.kompics.p2p.peer;

import java.math.BigInteger;

import se.sics.kompics.Event;

public class JoinPeer extends Event {

	private final BigInteger msPeerId;

//-------------------------------------------------------------------
	public JoinPeer(BigInteger msPeerId) {
		this.msPeerId = msPeerId;
	}

//-------------------------------------------------------------------
	public BigInteger getMSPeerId() {
		return this.msPeerId;
	}
}
