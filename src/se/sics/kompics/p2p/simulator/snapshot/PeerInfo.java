package se.sics.kompics.p2p.simulator.snapshot;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import p2p.system.peer.Peer;
import p2p.system.peer.PeerAddress;


public class PeerInfo {
	private PeerAddress self;	
	private PeerAddress pred;
	private PeerAddress succ;
	private Set<PeerAddress> longlinks = new HashSet<PeerAddress>();
	private PeerAddress[] succList = new PeerAddress[Peer.SUCC_SIZE];
	
	private Set<PeerAddress> mySubscribers = new HashSet<PeerAddress>();
	private HashMap<PeerAddress, Set<BigInteger>> receivedNotifications = new HashMap<PeerAddress, Set<BigInteger>>();
	private HashMap<PeerAddress, BigInteger> startingNumbers = new HashMap<PeerAddress, BigInteger>();
	private BigInteger myLastPublicationID = BigInteger.ZERO;
	private Set<BigInteger> mySubscriptions = new HashSet<BigInteger>();
	
	// This metric will include the activity of publication into consideration
	private int asSubscriberCount = 0;
	private int asForwarderCount = 0;
	private int tableSize = 0;
	
	// This metric will only consider how good the structure of the overlay is without considering the activity
	private Set<BigInteger> asSubscriberSet = new HashSet<BigInteger>();
	private Set<BigInteger> asForwarderSet = new HashSet<BigInteger>(); 
	// in asForwarderSet, we might include a peer which later on will be a subscriber.
	// So, to get the pure set of forwarder, we have to substract it with asSubsriberSet

//-------------------------------------------------------------------
	public PeerInfo(PeerAddress self) {
		this.self = self;
	}

//-------------------------------------------------------------------
	public void setPred(PeerAddress pred) {
		this.pred = pred;
	}

//-------------------------------------------------------------------
	public void setSucc(PeerAddress succ) {
		this.succ = succ;
	}

//-------------------------------------------------------------------
	public void setLonglinks(Set<PeerAddress> longlinks) {
		this.longlinks = longlinks;
	}

//-------------------------------------------------------------------
	public void setSuccList(PeerAddress[] succList) {
		for (int i = 0; i < succList.length; i++)
		this.succList[i] = succList[i];
	}

//-------------------------------------------------------------------
	public PeerAddress getSelf() {
		return this.self;
	}

//-------------------------------------------------------------------
	public PeerAddress getPred() {
		return this.pred;
	}

//-------------------------------------------------------------------
	public PeerAddress getSucc() {
		return this.succ;
	}

//-------------------------------------------------------------------
	public PeerAddress[] getLonglinks() {
		PeerAddress[] links = new PeerAddress[this.longlinks.size()];
		this.longlinks.toArray(links);
		return links;
	}
	
//-------------------------------------------------------------------
	public int getLonglinksSize() {
		return this.longlinks.size();
	}

//-------------------------------------------------------------------
	public PeerAddress[] getSuccList() {
		return this.succList;
	}

//-------------------------------------------------------------------
	public String toString() {
		String str = new String();
		String longlinks = new String();
		String succs = new String();
		
		/*
		longlinks = "[";
		for (int i = 0; i < Peer.LONGLINK_SIZE; i++)
			longlinks += this.longlinks[i] + ", ";
		longlinks += "]";
		*/
		longlinks = this.longlinks.toString();

		succs = "[";
		for (int i = 0; i < Peer.SUCC_SIZE; i++)
			succs += this.succList[i] + ", ";
		succs += "]";

		str += "peer: " + this.self;
		str += ", succ: " + this.succ;
		str += ", pred: " + this.pred;
		str += ", longlinks: " + longlinks;
		str += ", succList: " + succs;
		
		return str;
	}

//-------------------------------------------------------------------
// PUB/SUB related
	
	
	public void addSubscriber(PeerAddress subscriber) {
		this.mySubscribers.add(subscriber);		
	}
	
	public void removeSubscriber(PeerAddress subscriber) {
		this.mySubscribers.remove(subscriber);		
	}
	
	public Set<PeerAddress> getSubscribersList() {
		return this.mySubscribers;
	}

	public boolean addNotification(PeerAddress publisher, BigInteger notificationID) {
		Set<BigInteger> notificationList = receivedNotifications.get(publisher);
		
		if (notificationList == null) {
			notificationList = new HashSet<BigInteger>();
			receivedNotifications.put(publisher, notificationList);
		}
		
		if(notificationList.contains(notificationID))
			return false;

		notificationList.add(notificationID);
		return true;
		
		//receivedNotifications.put(publisher, notificationList);
	}
	
	public void setMyLastPublicationID(BigInteger id) {
		this.myLastPublicationID = id;
	}
	
	public boolean isPublisher() {
		return !this.myLastPublicationID.equals(BigInteger.ZERO);
	}

	public int areNotificationsComplete(PeerAddress publisher, BigInteger lastPublicationID) {
		Set<BigInteger> notificationList = this.receivedNotifications.get(publisher);

		if (notificationList == null)
			return lastPublicationID.intValue();

		int missingMessages = 0;
			
		BigInteger bi = BigInteger.ONE; // startingNumbers.get(publisher);
		while (!(bi.compareTo(lastPublicationID) == 1)) {
			if (!notificationList.contains(bi))
				missingMessages++;
			
			bi = bi.add(BigInteger.ONE);
		}
		
		return missingMessages;
	}
	
	public BigInteger getLastPublicationID() {
		return this.myLastPublicationID;
	}
	 
	public void setStartingNumber(PeerAddress publisher, BigInteger num) {
		this.startingNumbers.put(publisher, num);
	}
	
	public void setSubscriptions(Set<BigInteger> set) {
		this.mySubscriptions = set;
	}
	
	public Set<BigInteger> getSubscriptions() {
		return this.mySubscriptions;
	}

	public void incrementAsForwarderCount() {
		this.asForwarderCount++;
	}
	
	public void incrementAsSubscriberCount() {
		this.asSubscriberCount++;
	}

	public void addAsSubscriberSet(BigInteger topicID) {
		this.asSubscriberSet.add(topicID);
	}
	
	public void removeFromAsSubscriberSet(BigInteger topicID) {
		this.asSubscriberSet.remove(topicID);
	}
	
	public void addAsForwarderSet(BigInteger topicID) {
		this.asForwarderSet.add(topicID);
	}
	
	public void removeFromAsForwarderSet(BigInteger topicID) {
		this.asForwarderSet.remove(topicID);
	}
	
	public void setForwardingTableSize(int size) {
		this.tableSize = size;
	}
	
	public int getAsForwarderSetSize() {
		this.asForwarderSet.removeAll(this.asSubscriberSet);
		return this.asForwarderSet.size();
	}
	
	public int getAsSubscriberSetSize() {
		return this.asSubscriberSet.size();
	}
	
	public int getAsSubscriberCount() {
		return this.asSubscriberCount;
	}
	
	public int getAsForwarderCount() {
		return this.asForwarderCount;
	}
	
	public int getForwardingTableSize() {
		return this.tableSize;
	}
}
