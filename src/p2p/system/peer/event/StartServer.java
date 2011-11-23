package p2p.system.peer.event;

import java.math.BigInteger;

import se.sics.kompics.Event;

public class StartServer extends Event {

	private final BigInteger msPeerId;

//-------------------------------------------------------------------
	public StartServer(BigInteger msPeerId) {
		this.msPeerId = msPeerId;
	}

//-------------------------------------------------------------------
	public BigInteger getMSPeerId() {
		return this.msPeerId;
	}
}
