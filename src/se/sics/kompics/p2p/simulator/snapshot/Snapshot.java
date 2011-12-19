package se.sics.kompics.p2p.simulator.snapshot;

import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import p2p.simulator.scenarios.Scenario1;
import p2p.system.peer.Peer;
import p2p.system.peer.PeerAddress;

public class Snapshot {
	private static int counter = 0;
	private static TreeMap<PeerAddress, PeerInfo> peers = new TreeMap<PeerAddress, PeerInfo>();
	private static TreeMap<BigInteger, PeerAddress> peers2 = new TreeMap<BigInteger, PeerAddress>();
	private static Vector<PeerAddress> removedPeers = new Vector<PeerAddress>();
	private static String FILENAME = "peer.out";
	private static String DOTFILENAME = "peer.dot";
	private static String TRACESOUT = DateUtils.now() + "_" + Scenario1.NUMBER_OF_PEERS +  ".txt";
	private static String TRACE_HEADER = "#Settings: \n# Peers: "+Scenario1.NUMBER_OF_PEERS+" #Subscriptions: "+Scenario1.NUMBER_OF_SUBCRIPTIONS+" #Publications: "+Scenario1.NUMBER_OF_PUBLICATIONS
	+" #UnSubscriptions: "+Scenario1.NUMBER_OF_UNSUBSCRIPTIONS+" Subscription model: "+Scenario1.subscriptionsModel+ "\n"+
											"#1. counter\n" + 
											"#2. Unsubscribe request messages\n" + 
											"#3. Subscribe request messages\n" +
											"#4. * CONTROL MESSAGES\n" +
											"#5. * DEPTH OF TREE\n" +
											"#6. * HIT RATIO\n" +
											"#7. Forwarder nodes\n" +
											"#8. Subscriber nodes\n" +
											"#9. * RELAY NODE RATIO\n" +
											"#10. Forwarding action\n" +
											"#11. Subscribing action\n" +
											"#12. * FORWARDING OVERHEAD\n\n" +
											"#[1] \t[2] \t[3] \t[4] \t[5] \t[6] \t[7] \t[8] \t[9] \t[10] \t[11] \t[12]\n";
	private static HashMap<BigInteger, Integer> subscribeOverhead = new HashMap<BigInteger, Integer>();
	private static HashMap<BigInteger, Integer> unsubscribeOverhead = new HashMap<BigInteger, Integer>();
	private static HashMap<BigInteger, Vector<Integer>> multicastTree = new HashMap<BigInteger, Vector<Integer>>();
	private static int writetograph = 0;
	private static final int TICK = 10;
	private static final Random rand = new Random();
	private static final DecimalFormat df4 = new DecimalFormat("#.0000");
	private static final DecimalFormat df2 = new DecimalFormat("#.00");
	private static final DecimalFormat df0 = new DecimalFormat("#");
	static {
		FileIO.write("", FILENAME);
		FileIO.write("", DOTFILENAME);
		FileIO.write(TRACE_HEADER, TRACESOUT);
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
	public static void setLonglinks(PeerAddress address, Set<PeerAddress> longlinks) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.setLonglinks(longlinks);
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
		///*
		String str = new String();
		
		str += "current time: " + counter++ + "\n";
		str += reportNetworkState();
		str += reportDetailes();
		str += "###\n";
		
		System.out.println(str);
		
		FileIO.append(str, FILENAME);
		if(writetograph == TICK){
		generateGraphVizReport();
		writetograph = 0;
		}
		writetograph++;
		// */
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
		str += "longlink: " + verifyLonglinks(peersList) + "\n";
		//str += "succList: " + verifySuccList(peersList) + "\n";
		//str += details(peersList);
		
		int unsub = computeUnsubscribeOverhead();
		int sub = computeSubscribeOverhead();
		Vector<Double> depths = computeDepth();
		double hitratio = verifyNotifications(peersList);
		Vector<Double> relaynode = computeRelayNodeRatio(peersList);
		Vector<Double> forwardingOverhead = computeForwardingOverhead(peersList);
		str += "1. control message. T: " + (unsub + sub) + " U: " + unsub + " S: " + sub + "\n";
		str += "2. average multicast tree depth. " + depths + "\n";
		str += "3. hit ratio. notifications: " + hitratio + "\n";
		str += "4. relay node ratio: \t" + relaynode + "\n";
		str += "5. forwarding overhead:\t" + forwardingOverhead	+ "\n";
		
		String trace = counter + "\t" + 
						unsub + "\t" + 
						sub + "\t" +
						(unsub + sub) + "\t" +
						df4.format(depths.get(0)) + "\t" +
						df2.format(hitratio) + "\t" +
						df0.format(relaynode.get(0)) + "\t" +
						df0.format(relaynode.get(1)) + "\t" +
						df4.format(relaynode.get(2))  + "\t" +
						df0.format(forwardingOverhead.get(0)) + "\t" +
						df0.format(forwardingOverhead.get(1)) + "\t" +
						df4.format(forwardingOverhead.get(2))  + "\n";
						
		
		FileIO.append(trace, TRACESOUT);
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

	private static String verifyLonglinks(PeerAddress[] peersList) {
		int count = 0;
		String str = new String();
		
		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);
			
			count += (Peer.LONGLINK_SIZE - peer.getLonglinksSize());
				
		}
		
		if (count == 0)
			str += "longlinks list is correct :)";
		else
			str += count + " longlink(s) in ring are missing :(";
		
		return str;
	}
	
	/*
//-------------------------------------------------------------------
	private static String verifyFingers(PeerAddress[] peersList) {
		int count = 0;
		String str = new String();
		
		PeerAddress[] fingers = new PeerAddress[Peer.FINGER_SIZE];
		PeerAddress[] expectedFingers = new PeerAddress[Peer.FINGER_SIZE];
		
		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);
			fingers = peer.getLonglinks();
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
*/
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

	/*
//-------------------------------------------------------------------
	private static PeerAddress[] getExpectedFingers(PeerAddress peer, PeerAddress[] peersList) {
		BigInteger index;
		BigInteger id;
		PeerAddress[] expectedFingers = new PeerAddress[Peer.LONGLINK_SIZE];
		
		for (int i = 0; i < Peer.LONGLINK_SIZE; i++) {
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
	*/

//-------------------------------------------------------------------
	private static PeerAddress[] getExpectedSuccList(PeerAddress peer, PeerAddress[] peersList) {
		int index = 0;
		PeerAddress[] expectedSuccList = new PeerAddress[Peer.LONGLINK_SIZE];
		
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
		
		String neighborLabel = new String();
		String predSuccColor = "black";

		int i = 0;
		for (PeerAddress peer : peers.keySet()) {
			PeerAddress succ = peers.get(peer).getSucc();
			PeerAddress pred = peers.get(peer).getPred();
			peerLabel = peer.getPeerId().toString();

			str += peer.getPeerId() + " [ color = " + color[i] + ", style = filled, label = \"" + peerLabel + "\" ];\n";

			if (succ != null) {
				succLabel = succ.getPeerId().toString();
				str += succ.getPeerId() + " [ style = filled, label = \""
						+ succLabel + "\" ];\n";
				str += peer.getPeerId() + "--" + succ.getPeerId()
						+ "[ color = " + predSuccColor + " ];\n";
			}

			if (pred != null) {
				predLabel = pred.getPeerId().toString();
				str += pred.getPeerId() + " [ style = filled, label = \""
						+ predLabel + "\" ];\n";
				str += peer.getPeerId() + "--" + pred.getPeerId()
						+ "[ color = " + predSuccColor + " ];\n";
			}
			
			
			PeerInfo info = peers.get(peer);
			//int size = info.getLonglinksSize()+info.getFriendlinksSize();
			PeerAddress[] longlinks = new PeerAddress[info.getLonglinksSize()];
			//Set<PeerAddress> friendlinks = new HashSet<PeerAddress>();
			//PeerInfo peer = peers.get(peersList[j]);
			longlinks = info.getLonglinks();
			 //friendlinks = info.getFriendlinks();
			//expectedFingers = getExpectedFingers(peer.getSelf(), peersList);
			
			for (int j = 0; j < info.getLonglinksSize(); j++) {
				if (longlinks[j] != null ){
					neighborLabel = longlinks[j].getPeerId().toString();
					str += longlinks[j].getPeerId() + " [ style = filled, label = \"" + neighborLabel + "\" ];\n";
					str += peer.getPeerId() + "--" + longlinks[j].getPeerId() + "[ color = " + color[i] + " ];\n";
					}
			}
/*
			Iterator<PeerAddress> itr = friendlinks.iterator();
		//	for (int j = 0; j < info.getFriendlinksSize(); j++) {
				while(itr.hasNext() ){
					PeerAddress friend = itr.next();
					if(friend!=null){
						neighborLabel = friend.getPeerId().toString();
						str += friend.getPeerId() + " [ style = filled, label = \"" + neighborLabel + "\" ];\n";
						str += peer.getPeerId() + "--" + friend.getPeerId() + "[ color = " + color[i] + " ];\n";
					}
				}
			*/
			
			i = (i + 1) % color.length;
		}
		
		str += "}\n\n";

		FileIO.write(str, DOTFILENAME);
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

	// Decrement my subscribers
	public static void removeSubscription(BigInteger topicID,
			PeerAddress subscriber) {
		PeerInfo peerInfo = peers.get(peers2.get(topicID));

		if (peerInfo == null)
			return;

		peerInfo.removeSubscriber(subscriber);
	}

	// Peer has received notification as subscriber - Traffic overhead related
	public static void receiveNotification(BigInteger topicID, PeerAddress subscriber, BigInteger notificationID) {
		PeerInfo peerInfo = peers.get(subscriber);

		if (peerInfo == null)
			return;

		if (peerInfo.addNotification(peers2.get(topicID), notificationID))
			peerInfo.incrementAsSubscriberCount();
	}
	
	// Peer has received notification as forwarder in relay path - Traffic overhead related
	public static void forwardNotification(BigInteger topicID, PeerAddress forwarder, BigInteger notificationID) {
		PeerInfo peerInfo = peers.get(forwarder);

		if (peerInfo == null)
			return;

		peerInfo.incrementAsForwarderCount();
	}
	
	// Peer is participating as forwarder for a topic in the relay path - Structure related
	public static void becomesForwarder(BigInteger topicID, PeerAddress forwarder) {
		PeerInfo peerInfo = peers.get(forwarder);

		if (peerInfo == null)
			return;

		peerInfo.addAsForwarderSet(topicID);
	}

	// Peer is participating as subscriber for a topic in the relay path - Structure related

	public static void becomesSubscriber(BigInteger topicID, PeerAddress subscriber) {
		PeerInfo peerInfo = peers.get(subscriber);

		if (peerInfo == null)
			return;
		
		peerInfo.addAsSubscriberSet(topicID);
	}
	
	// should it be synchronized?
	public static void addToSubscribeTree(BigInteger topicID) {
		int count = 0;
		if (subscribeOverhead.get(topicID) != null)	
		count = subscribeOverhead.get(topicID);

		count++;
		subscribeOverhead.put(topicID, count);
		
		
	}
	
	public static void addToUnsubscribeOverhead(BigInteger topicID) {
		int count = 0;
		if (unsubscribeOverhead.get(topicID) != null)	
		count = unsubscribeOverhead.get(topicID);

		count++;
		unsubscribeOverhead.put(topicID, count);
	}
	
	public static int computeUnsubscribeOverhead() {
		
		Collection<Integer> set = unsubscribeOverhead.values();
	
		int count = sum(set);
		
		return count;//+" " +set.toString();
	}
	
	private static int sum(Collection<Integer> set) {
		int count = 0;
		
		Iterator<Integer> itr =set.iterator();
		while(itr.hasNext())
		{
			count += itr.next();	
		}
		
		return count;
	}
	
	private static double average(Collection<Integer> set) {
		double sum = sum(set);
		
		return sum / set.size();
	}
	
	public static int computeSubscribeOverhead() {
		
		Collection<Integer> set = subscribeOverhead.values();
	
		int count = sum(set);
		
		return count; //+" " +set.toString();
	}
	
	// Max Length of multicast tree of each topic - called from rendezvous peer only
	public static void addDepthToMulticastTree(BigInteger topicId, int length){
		Vector<Integer> depths = multicastTree.get(topicId);
		
		if (depths != null){
			depths.add(length);	
		}
		else{
			depths = new Vector<Integer>();
			depths.add(length);
		}
		multicastTree.put(topicId, depths);
	}
	
	public static Vector<Double> computeDepth(){
		Collection<Vector<Integer>> values = multicastTree.values();
		Iterator<Vector<Integer>> iter = values.iterator();
		
		Vector<Double> avgDepths = new  Vector<Double>();
		
		double avgSystemDepth = 0;
		double avgDepth;
		while(iter.hasNext()) {
			Vector<Integer> depths = iter.next();
			avgDepth = average(depths);
			avgDepths.add(avgDepth);
			
			avgSystemDepth += avgDepth;
		}
		
		avgSystemDepth /= multicastTree.size();
		avgDepths.add(0, avgSystemDepth);
		return avgDepths;
	}
		
	// Publisher sets the last sequence no. that it published
	public static void publish(PeerAddress publisher, BigInteger publicationID) {
		PeerInfo peerInfo = peers.get(publisher);

		if (peerInfo == null)
			return;

		peerInfo.setMyLastPublicationID(publicationID);
	}

	// Calculate hit ratio for notifications
	private static double verifyNotifications(PeerAddress[] peersList) {
		int wrongs[] = new int[peersList.length];
		
		int totalMissingNotifications = 0;
		int totalNotifications = 0;

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

					if (peer2 == null)
						continue;

					totalMissingNotifications += peer2.areNotificationsComplete(peer.getSelf(), peer.getLastPublicationID());
					
				}
				totalNotifications += (subscribersList.size() * peer.getLastPublicationID().intValue());
			}
		}
		
		double hitratio = 1 - (((double) totalMissingNotifications) / totalNotifications);

		return hitratio; // + " T:" + totalNotifications + " M:" + totalMissingNotifications;

	}
	
	public static int computeTotalForwardersCount(PeerAddress[] peersList) {
		int count = 0;

		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);

			if (peer == null)
				continue;
			
			count += peer.getAsForwarderCount();
		}
		
		return count;
	}
	
	public static int computeTotalSubscribersCount(PeerAddress[] peersList) {
		int count = 0;

		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);

			if (peer == null)
				continue;
			
			count += peer.getAsSubscriberCount();
		}
		
		return count;
	}

	public static Vector<Double> computeForwardingOverhead(PeerAddress[] peersList) {
		double F = computeTotalForwardersCount(peersList);
		double S = computeTotalSubscribersCount(peersList);
		double ratio = F / S;
		
		Vector<Double> result = new Vector<Double>();
		result.add(F);
		result.add(S);
		result.add(ratio);
		
		return result;
	}

	public static int computeTotalForwardersSet(PeerAddress[] peersList) {
		int count = 0;

		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);

			if (peer == null)
				continue;
			
			count += peer.getAsForwarderSetSize();
		}
		
		return count;
	}
	
	public static int computeTotalSubscribersSet(PeerAddress[] peersList) {
		int count = 0;

		for (int i = 0; i < peersList.length; i++) {
			PeerInfo peer = peers.get(peersList[i]);

			if (peer == null)
				continue;
			
			count += peer.getAsSubscriberSetSize();
		}
		
		return count;
	}
	
	/**
	 * 
	 * @param peersList
	 * @return Vector Double of size 3: Forwarders, Subscribers, Ratio
	 */
	public static Vector<Double> computeRelayNodeRatio(PeerAddress[] peersList) {
		Vector<Double> result = new Vector<Double>();
		double F = computeTotalForwardersSet(peersList);
		double S = computeTotalSubscribersSet(peersList);
		double overhead =  F / S;
		
		result.add(F);
		result.add(S);
		result.add(overhead);
				
		return result;
							
	}
	
	// Set a peers own subscriptions
	public static void setPeerSubscriptions(PeerAddress peer, Set<BigInteger> subscriptions) {
		PeerInfo peerInfo = peers.get(peer);

		if (peerInfo == null)
			return;

		peerInfo.setSubscriptions(subscriptions);
	}
	
	

	
	private static double average(double[] num) {
		double sum = 0.0;
		
		for (int i = 0; i < num.length; i++) {
			sum += num[i];
		}
		return sum/ (double) num.length;
	}
	
	private static double computeSimilarityIndex(Set<BigInteger> subscriptions1, Set<BigInteger> subscriptions2) {
		double sameTopicIDs = 0.0;
		double union = 1.0;
		
		Iterator<BigInteger> itr = subscriptions2.iterator();
		while (itr.hasNext()) {
			if (subscriptions1.contains(itr.next()))
				sameTopicIDs++;
		}
		
		if (sameTopicIDs == 0) {
			return 0;
		}
		else {
			union = subscriptions1.size() + subscriptions2.size() - sameTopicIDs;
			return ((double) sameTopicIDs) / ((double) union);
		}
	}

}
