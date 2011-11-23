package p2p.system.peer;

import se.sics.kompics.network.Message;

public class PeerMessage extends Message {

	private static final long serialVersionUID = -6815596147580962155L;

	private final PeerAddress source;
	private final PeerAddress destination;
	private final PeerAddress newFriend;

//-------------------------------------------------------------------	
	public PeerMessage(PeerAddress source, PeerAddress destination, PeerAddress newFriend) {
		super(source.getPeerAddress(), destination.getPeerAddress());
		this.source = source;
		this.destination = destination;
		this.newFriend = newFriend;
	}

//-------------------------------------------------------------------	
	public PeerAddress getMSPeerDestination() {
		return destination;
	}

//-------------------------------------------------------------------	
	public PeerAddress getMSPeerSource() {
		return source;
	}

//-------------------------------------------------------------------	
	public PeerAddress getNewFriend() {
		return newFriend;
	}
}

