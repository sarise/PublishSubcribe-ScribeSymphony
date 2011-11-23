package p2p.simulator.snapshot_unused;


import java.util.Vector;

import p2p.system.peer.PeerAddress;



public class PeerInfo {
	private Vector<PeerAddress> friends = new Vector<PeerAddress>();

//-------------------------------------------------------------------
	public void addFriends(Vector<PeerAddress> friends) {
		this.friends.addAll(friends);
	}

//-------------------------------------------------------------------
	public void addFriend(PeerAddress friend) {
		if (!this.friends.contains(friend))
			this.friends.addElement(friend);
	}

//-------------------------------------------------------------------
	public void removeFriend(PeerAddress friend) {
		this.friends.removeElement(friend);
	}

//-------------------------------------------------------------------
	public Vector<PeerAddress> getFriends() {
		return this.friends;
	}
}
