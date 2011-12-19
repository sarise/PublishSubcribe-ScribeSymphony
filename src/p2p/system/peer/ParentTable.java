package p2p.system.peer;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import se.sics.kompics.address.Address;

public class ParentTable {
	private HashMap<Address, Set<BigInteger>> parentTable = new HashMap<Address, Set<BigInteger>>();

	// HashMap: link --> a set of TopicID

	public ParentTable() {

	}

	private void addLink(Address peer) {
		if (this.parentTable.containsKey(peer))
			return;

		this.parentTable.put(peer, new HashSet<BigInteger>());
	}

	/**
	 * 
	 * @param oldPeer
	 * @param newPeer
	 * @return a set of TopicID which needs to be re-subscribed.
	 */
	private Set<BigInteger> changeLink(Address oldPeer, Address newPeer) {
		if (oldPeer == null) {
			addLink(newPeer);
			return new HashSet<BigInteger>();
		}
		
		if (oldPeer.equals(newPeer))
			return new HashSet<BigInteger>();

		Set<BigInteger> set = this.parentTable.get(oldPeer);

		//this.parentTable.put(newPeer, new HashSet<BigInteger>());
		this.parentTable.remove(oldPeer);
		return set;
	}
	
	public Set<BigInteger> changeLink(PeerAddress oldPeer, Address newPeer) {
		if (oldPeer == null) {
			addLink(newPeer);
			return new HashSet<BigInteger>();
		}
		else {
			return changeLink(oldPeer.getPeerAddress(), newPeer);
		}
	}

	public void addTopicID(Address peer, BigInteger topicID) {
		Set<BigInteger> set = this.parentTable.get(peer);

		if (set == null)
			set = new HashSet<BigInteger>();

		set.add(topicID);

		this.parentTable.put(peer, set);
	}

	public Set<Address> removeTopic(BigInteger topicID) {
		
		Set<Address> oldLinks = new HashSet<Address>();
		
		Iterator<Address> iter = this.parentTable.keySet().iterator();
		Address address;
		Set<BigInteger> topicIDs;
		
		while (iter.hasNext()) {
			address = iter.next();
			topicIDs = this.parentTable.get(address);
			if (topicIDs.remove(topicID))
				oldLinks.add(address);
		}
		
		return oldLinks;		
	}
	
	public Set<BigInteger> removeLink(Address peer) {
		Set<BigInteger> set = this.parentTable.get(peer);

		this.parentTable.remove(peer);
		return set;
	}
	

}
