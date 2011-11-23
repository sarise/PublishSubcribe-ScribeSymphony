package p2p.system.peer;

import p2p.system.peer.event.JoinPeer;
import p2p.system.peer.event.PublishPeer;
import p2p.system.peer.event.StartServer;
import p2p.system.peer.event.SubscribePeer;
import p2p.system.peer.event.SubscriptionInit;
import p2p.system.peer.event.UnsubscribePeer;
import se.sics.kompics.PortType;

public class PeerPort extends PortType {{
	negative(JoinPeer.class);
	negative(StartServer.class);
	negative(SubscribePeer.class);
	negative(UnsubscribePeer.class);
	negative(PublishPeer.class);
	negative(SubscriptionInit.class);
}}
