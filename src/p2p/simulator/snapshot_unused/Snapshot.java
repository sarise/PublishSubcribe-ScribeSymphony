package p2p.simulator.snapshot_unused;


import java.util.HashMap;
import java.util.Vector;

import p2p.system.peer.PeerAddress;



public class Snapshot {
	private static int counter = 0;
	private static HashMap<PeerAddress, PeerInfo> peers = new HashMap<PeerAddress, PeerInfo>();
	private static String FILENAME = "peer.out";
	private static String DOTFILENAME = "peer.dot";
	private static Vector<PeerAddress> removedPeers = new Vector<PeerAddress>();

	static {
		FileIO.write("", FILENAME);
		FileIO.write("", DOTFILENAME);
	}

//-------------------------------------------------------------------
	public static void addPeer(PeerAddress address) {
		peers.put(address, new PeerInfo());
	}

//-------------------------------------------------------------------
	public static void removePeer(PeerAddress address) {
		peers.remove(address);
		removedPeers.addElement(address);
	}

//-------------------------------------------------------------------
	public static void addFriends(PeerAddress address, Vector<PeerAddress> friends) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.addFriends(friends);
	}

//-------------------------------------------------------------------
	public static void addFriend(PeerAddress address, PeerAddress friend) {
		PeerInfo peerInfo = peers.get(address);
		
		if (peerInfo == null)
			return;
		
		peerInfo.addFriend(friend);
	}

//-------------------------------------------------------------------
	public static void removeFriend(PeerAddress address, PeerAddress friend) {
		PeerInfo peerInfo = peers.get(address);

		if (peerInfo == null)
			return;

		peerInfo.removeFriend(friend);
	}

//-------------------------------------------------------------------
	public static void report() {
		clean();
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
		PeerInfo peerInfo;
		String str = new String();

		for (PeerAddress peer : peers.keySet()) {
			peerInfo = peers.get(peer);
		
			str += "peer: " + peer;
			str += ", friends: " + peerInfo.getFriends();
			str += "\n";
		}
		
		return str;
	}

//-------------------------------------------------------------------
	private static void clean() {
		for (PeerAddress peer : peers.keySet()) {
			for (PeerAddress removed : removedPeers)
				peers.get(peer).removeFriend(removed);
		}
	}

//-------------------------------------------------------------------
	private static void generateGraphVizReport() {
		String str = "digraph g {\n";
		String[] color = {"red3", "cornflowerblue", "darkgoldenrod1", "darkolivegreen2", "steelblue", "gold", "darkseagreen4", "peru",
						"palevioletred3", "royalblue3", "orangered", "darkolivegreen4", "chocolate3", "cadetblue3", "orange"};
		String srcLabel = new String();
		String destLabel = new String();
		
		int i = 0;
		for (PeerAddress peer : peers.keySet()) {
			Vector<PeerAddress> friends = peers.get(peer).getFriends();
			for (PeerAddress friend : friends) {
				srcLabel = peer.getPeerId().toString();
				destLabel = friend.getPeerId().toString();
				str += peer.getPeerId() + " [ color = " + color[i] + ", style = filled, label = \"" + srcLabel + "\" ];\n";
				str += friend.getPeerId() + " [ style = filled, label = \"" + destLabel + "\" ];\n";
				str += peer.getPeerId() + "->" + friend.getPeerId() + "[ color = " + color[i] + " ];\n";
			}
			
			i = (i + 1) % color.length;
		}
		
		str += "}\n\n";
		
		FileIO.append(str, DOTFILENAME);
	}		
}
