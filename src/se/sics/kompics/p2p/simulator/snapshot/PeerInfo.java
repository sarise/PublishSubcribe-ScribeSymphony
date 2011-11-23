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
	private PeerAddress[] fingers = new PeerAddress[Peer.FINGER_SIZE];
	private PeerAddress[] succList = new PeerAddress[Peer.SUCC_SIZE];
	
	private Set<PeerAddress> mySubscribers = new HashSet<PeerAddress>();
	private HashMap<PeerAddress, Set<BigInteger>> receivedNotifications = new HashMap<PeerAddress, Set<BigInteger>>();
	private HashMap<PeerAddress, BigInteger> startingNumbers = new HashMap<PeerAddress, BigInteger>();
	private BigInteger myLastPublicationID = BigInteger.ZERO;

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
	public void setFingers(PeerAddress[] fingers) {
		for (int i = 0; i < fingers.length; i++)
		this.fingers[i] = fingers[i];
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
	public PeerAddress[] getFingers() {
		return this.fingers;
	}

//-------------------------------------------------------------------
	public PeerAddress[] getSuccList() {
		return this.succList;
	}

//-------------------------------------------------------------------
	public String toString() {
		String str = new String();
		String finger = new String();
		String succs = new String();
		
		finger = "[";
		for (int i = 0; i < Peer.FINGER_SIZE; i++)
			finger += this.fingers[i] + ", ";
		finger += "]";

		succs = "[";
		for (int i = 0; i < Peer.SUCC_SIZE; i++)
			succs += this.succList[i] + ", ";
		succs += "]";

		str += "peer: " + this.self;
		str += ", succ: " + this.succ;
		str += ", pred: " + this.pred;
		str += ", fingers: " + finger;
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

	public void addNotification(PeerAddress publisher, BigInteger notificationID) {
		Set<BigInteger> notificationList = receivedNotifications.get(publisher);
		
		if (notificationList == null)
			notificationList = new HashSet<BigInteger>();
		
		notificationList.add(notificationID);
	}
	
	public void setMyLastPublicationID(BigInteger id) {
		this.myLastPublicationID = id;
	}
	
	public boolean isPublisher() {
		return !this.myLastPublicationID.equals(BigInteger.ZERO);
	}
	
	public boolean areNotificationsComplete(PeerAddress publisher, BigInteger lastPublicationID) {
		Set<BigInteger> notificationList = this.receivedNotifications.get(publisher);

		if (notificationList == null)
			return false;
		
		BigInteger bi = BigInteger.ONE; //startingNumbers.get(publisher);
		while (!(bi.compareTo(lastPublicationID) == 1)) {
			if (!notificationList.contains(bi))
				return false;
			bi = bi.add(BigInteger.ONE);
		}
		return true;
	}
	
	public BigInteger getLastPublicationID() {
		return this.myLastPublicationID;
	}
	 
	public void setStartingNumber(PeerAddress publisher, BigInteger num) {
		this.startingNumbers.put(publisher, num);
	}
	

}
