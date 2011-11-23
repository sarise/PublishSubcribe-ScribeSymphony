package se.sics.kompics.p2p.simulator.snapshot;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import p2p.system.peer.Peer;
import p2p.system.peer.PeerAddress;



public class Snapshot {
	private static int counter = 0;
	private static TreeMap<PeerAddress, PeerInfo> peers = new TreeMap<PeerAddress, PeerInfo>();
	private static TreeMap<BigInteger, PeerAddress> peers2 = new TreeMap<BigInteger, PeerAddress>();
	private static Vector<PeerAddress> removedPeers = new Vector<PeerAddress>();
	private static String FILENAME = "peer.out";
	private static String DOTFILENAME = "peer.dot";

	static {
		FileIO.write("", FILENAME);
		FileIO.write("", DOTFILENAME);
	}

//-------------------------------------------------------------------
	public static void addPeer(PeerAddress address) {
		peers.put(address, new PeerInfo(address));
		peers2.put(address.getPeerId(), address);
	}

//-------------------------------------------------------------------
	public static void removePeer(PeerAddress address) {
		peers.remove(address);
		peers2.remove(address.getPeerId());
		removedPeers.addElement(address);
	}

//-------------------------------------------------------------------
	public static void setSucc(PeerAddress address, PeerAddress succ) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.setSucc(succ);
	}

//-------------------------------------------------------------------
	public static void setPred(PeerAddress address, PeerAddress pred) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.setPred(pred);
	}
	
//-------------------------------------------------------------------
	public static void setFingers(PeerAddress address, PeerAddress[] fingers) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.setFingers(fingers);
	}

//-------------------------------------------------------------------
	public static void setSuccList(PeerAddress address, PeerAddress[] succList) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.setSuccList(succList);
	}

//-------------------------------------------------------------------
	public static void report() {
		String str = new String();
		str += "current time: " + counter++ + "\n";
		str += reportNetworkState();
		str += reportDetailes();
		str += "###\n";
		
		System.out.println(str);
		
		FileIO.append(str, FILENAME);
		generateGraphVizReport();
	}

//-------------------------------------------------------------------
	private static String reportNetworkState() {
		return "total number of peers: " + peers.size() + "\n";
	}
	
//-------------------------------------------------------------------
	private static String reportDetailes() {
		PeerAddress[] peersList = new PeerAddress[peers.size()];
		peers.keySet().toArray(peersList);
		
		String str = new String();
		str += "ring: " + verifyRing(peersList) + "\n";
		str += "reverse ring: " + verifyReverseRing(peersList) + "\n";
		str += "fingers: " + verifyFingers(peersList) + "\n";
		str += "succList: " + verifySuccList(peersList) + "\n";
		//str += details(peersList);
		
		str += "notifications: " + verifyNotifications(peersList) + "\n";
		
		return str;
	}

//-------------------------------------------------------------------
	private static String verifyRing(PeerAddress[] peersList) {
		int count = 0;
		String str = new String();

		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);
			
			if (i == peersList.length - 1) {
				if (peer.getSucc() == null || !peer.getSucc().equals(peersList[0]))
					count++;
			} else if (peer.getSucc() == null || !peer.getSucc().equals(peersList[i + 1]))
				count++;			
		}
		
		if (count == 0)
			str = "ring is correct :)";
		else
			str = count + " succ link(s) in ring are wrong :(";
		
		return str;
	}

//-------------------------------------------------------------------
	private static String verifyReverseRing(PeerAddress[] peersList) {
		int count = 0;
		String str = new String();
		
		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);
			
			if (i == 0) {
				if (peer.getPred() == null || !peer.getPred().equals(peersList[peersList.length - 1]))
					count++;
			} else {
				if (peer.getPred() == null || !peer.getPred().equals(peersList[i - 1]))
					count++;
			}
		}
		
		if (count == 0)
			str = "reverse ring is correct :)";
		else
			str = count + " pred link(s) in ring are wrong :(";
		
		return str;
	}

//-------------------------------------------------------------------
	private static String verifyFingers(PeerAddress[] peersList) {
		int count = 0;
		String str = new String();
		
		PeerAddress[] fingers = new PeerAddress[Peer.FINGER_SIZE];
		PeerAddress[] expectedFingers = new PeerAddress[Peer.FINGER_SIZE];
		
		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);
			fingers = peer.getFingers();
			expectedFingers = getExpectedFingers(peer.getSelf(), peersList);
			
			for (int j = 0; j < Peer.FINGER_SIZE; j++) {
				if (fingers[j] == null || (fingers[j] != null && !fingers[j].equals(expectedFingers[j])))
					count++;
			}
		}
		
		if (count == 0)
			str += "finger list is correct :)";
		else
			str += count + " link(s) in finger list are wrong :(";
		
		return str;
	}

//-------------------------------------------------------------------
	private static String verifySuccList(PeerAddress[] peersList) {
		int count = 0;
		String str = new String();
	
		PeerAddress[] succList = new PeerAddress[Peer.SUCC_SIZE];
		PeerAddress[] expectedSuccList = new PeerAddress[Peer.SUCC_SIZE];
				
		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);
			succList = peer.getSuccList();
			expectedSuccList = getExpectedSuccList(peer.getSelf(), peersList);

			for (int j = 0; j < Peer.SUCC_SIZE; j++) {
				if (succList[j] == null || (succList[j] != null && !succList[j].equals(expectedSuccList[j])))
					count++;
			}
		}
		
		if (count == 0)
			str = "successor list is correct :)";
		else
			str = count + " link(s) in successor list are wrong :(";
		
		return str;
	}

//-------------------------------------------------------------------
	private static PeerAddress[] getExpectedFingers(PeerAddress peer, PeerAddress[] peersList) {
		BigInteger index;
		BigInteger id;
		PeerAddress[] expectedFingers = new PeerAddress[Peer.FINGER_SIZE];
		
		for (int i = 0; i < Peer.FINGER_SIZE; i++) {
			index = new BigInteger(2 + "").pow(i);			
			id = peer.getPeerId().add(index).mod(Peer.RING_SIZE); 

			for (int j = 0; j < peersList.length; j++) {
				if (peersList[j].getPeerId().compareTo(id) != -1) {
					expectedFingers[i] = new PeerAddress(peersList[j]);
					break;
				}
			}
			
			if (expectedFingers[i] == null)
				expectedFingers[i] = new PeerAddress(peersList[0]);
		}
		
		return expectedFingers;
	}

//-------------------------------------------------------------------
	private static PeerAddress[] getExpectedSuccList(PeerAddress peer, PeerAddress[] peersList) {
		int index = 0;
		PeerAddress[] expectedSuccList = new PeerAddress[Peer.FINGER_SIZE];
		
		for (int i = 0; i < peersList.length; i++) {
			if (peersList[i].getPeerId().compareTo(peer.getPeerId()) == 1) {
				index = i;
				break;
			}
		}

		for (int i = 0; i < Peer.SUCC_SIZE; i++)
			expectedSuccList[i] = peersList[(index + i) % peersList.length];
			
		return expectedSuccList;
	}

//-------------------------------------------------------------------
	private static void generateGraphVizReport() {
		String str = "graph g {\n";
		String[] color = {"red3", "cornflowerblue", "darkgoldenrod1", "darkolivegreen2", "steelblue", "gold", "darkseagreen4", "peru",
						"palevioletred3", "royalblue3", "orangered", "darkolivegreen4", "chocolate3", "cadetblue3", "orange"};
		String peerLabel = new String();
		String succLabel = new String();
		String predLabel = new String();
		
		int i = 0;
		for (PeerAddress peer : peers.keySet()) {
			PeerAddress succ = peers.get(peer).getSucc();
			PeerAddress pred = peers.get(peer).getPred();
			peerLabel = peer.getPeerId().toString();

			str += peer.getPeerId() + " [ color = " + color[i] + ", style = filled, label = \"" + peerLabel + "\" ];\n";

			if (succ != null) {
				succLabel = succ.getPeerId().toString();
				str += succ.getPeerId() + " [ style = filled, label = \"" + succLabel + "\" ];\n";
				str += peer.getPeerId() + "--" + succ.getPeerId() + "[ color = " + color[i] + " ];\n";
			}

			if (pred != null) {
				predLabel = pred.getPeerId().toString();
				str += pred.getPeerId() + " [ style = filled, label = \"" + predLabel + "\" ];\n";
				str += peer.getPeerId() + "--" + pred.getPeerId() + "[ color = " + color[i] + " ];\n";
			}
			
			i = (i + 1) % color.length;
		}
		
		str += "}\n\n";
		
		FileIO.append(str, DOTFILENAME);
	}
	
//------------------------
// PUB/SUB related checking -- HIT RATIO
	
	public static void addSubscription(BigInteger topicID, PeerAddress subscriber, BigInteger lastSequenceNum) {
		PeerInfo peerInfo = peers.get(peers2.get(topicID));
		
		if (peerInfo == null) return;
		
		peerInfo.addSubscriber(subscriber);
		
		PeerInfo peerInfo2 = peers.get(subscriber);
		peerInfo2.setStartingNumber(peers2.get(topicID), lastSequenceNum);
	}
	
	public static void removeSubscription(BigInteger topicID, PeerAddress subscriber) {
		PeerInfo peerInfo = peers.get(peers2.get(topicID));
		
		if (peerInfo == null)
			return;
		
		peerInfo.removeSubscriber(subscriber);
	}
	
	public static void receiveNotification(BigInteger topicID, PeerAddress subscriber, BigInteger notificationID) {
		PeerInfo peerInfo = peers.get(subscriber);
		
		if (peerInfo == null)
			return;
		
		peerInfo.addNotification(peers2.get(topicID), notificationID);
	}
	
	public static void publish(PeerAddress publisher, BigInteger publicationID) {
		PeerInfo peerInfo = peers.get(publisher);
		
		if (peerInfo == null)
			return;
		
		peerInfo.setMyLastPublicationID(publicationID);
	}
	
	private static String verifyNotifications(PeerAddress[] peersList) {
		String str = "";
		int wrongs[] = new int[peersList.length];
		
		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);
			
			if (peer == null) continue;
			
			wrongs[i] = -1;
			
			if (peer.isPublisher()) {
				wrongs[i] = 0;
				
				Set<PeerAddress> subscribersList = peer.getSubscribersList();
				Iterator<PeerAddress> iter = subscribersList.iterator();
			    while (iter.hasNext()) {
			    	PeerAddress subscriber = iter.next();
			    	PeerInfo peer2 = peers.get(subscriber);
			    	
			    	if (peer2 == null) continue;
			    	
			    	if (!peer2.areNotificationsComplete(peer.getSelf(), peer.getLastPublicationID()))
			    		wrongs[i]++;
			    }
			}
		}
		
		return Arrays.toString(wrongs);
		
	}
	
}
