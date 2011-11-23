package p2p.simulator.core;


import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import p2p.main.Configuration;
import p2p.simulator.core.event.AllPeerJoin;
import p2p.simulator.core.event.AllPeerSubscribe_C;
import p2p.simulator.core.event.AllPeerSubscribe_T;
import p2p.simulator.core.event.PeerFail;
import p2p.simulator.core.event.PeerJoin;
import p2p.simulator.core.event.PeerPublish;
import p2p.simulator.core.event.PeerSubscribe;
import p2p.simulator.core.event.PeerUnsubscribe;
import p2p.simulator.core.event.ServerStart;
import p2p.simulator.scenarios.Scenario1;
import p2p.system.peer.Peer;
import p2p.system.peer.PeerAddress;
import p2p.system.peer.PeerConfiguration;
import p2p.system.peer.PeerInit;
import p2p.system.peer.PeerPort;
import p2p.system.peer.event.JoinPeer;
import p2p.system.peer.event.PublishPeer;
import p2p.system.peer.event.StartServer;
import p2p.system.peer.event.SubscribePeer;
import p2p.system.peer.event.SubscriptionInit;
import p2p.system.peer.event.UnsubscribePeer;


import se.sics.kompics.ChannelFilter;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorConfiguration;
import se.sics.kompics.p2p.simulator.snapshot.Snapshot;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

public final class Simulator extends ComponentDefinition {

	private final String TWITTER_DATASET_FILENAME = "config/graph.sub";
	private final int DATASET_LENGTH = 100;
	
	Positive<SimulatorPort> simulator = positive(SimulatorPort.class);
	Positive<Network> network = positive(Network.class);
	Positive<Timer> timer = positive(Timer.class);

	private int peerIdSequence;

	private Address peer0Address;
	private BigInteger idSpaceSize;
	private ConsistentHashtable<BigInteger> view;
	private final HashMap<BigInteger, Component> peers;
	private final HashMap<BigInteger, PeerAddress> peersAddress;
	
	
	private Component server;
	private PeerAddress serverPeerAddress;
	
	private BootstrapConfiguration bootstrapConfiguration;
	private PeerConfiguration peerConfiguration;	
	private PingFailureDetectorConfiguration fdConfiguration;
	
	private Set<BigInteger>[] blocks; 										// for correlated subscription model
	private Set<BigInteger> twitterIDs;											// for twitter 
	private HashMap<BigInteger, Set<BigInteger>> subscriptionListForAllNodes;	// for twitter

//-------------------------------------------------------------------	
	public Simulator() {
		peers = new HashMap<BigInteger, Component>();
		peersAddress = new HashMap<BigInteger, PeerAddress>();
		view = new ConsistentHashtable<BigInteger>();

		subscribe(handleInit, control);
		
		subscribe(handleGenerateReport, timer);
		
		subscribe(handlePeerJoin, simulator);
		subscribe(handleAllPeerJoin, simulator);
		subscribe(handlePeerFail, simulator);
		subscribe(handlePeerSubscribe, simulator);
		subscribe(handlePeerUnsubscribe, simulator);
		subscribe(handlePeerPublish, simulator);
		subscribe(handleAllPeerSubscribe_T, simulator);
		subscribe(handleAllPeerSubscribe_C, simulator);
	}

//-------------------------------------------------------------------	
	Handler<SimulatorInit> handleInit = new Handler<SimulatorInit>() {
		public void handle(SimulatorInit init) {
			peers.clear();
			peerIdSequence = 0;

			peer0Address = init.getPeer0Address();
			bootstrapConfiguration = init.getBootstrapConfiguration();
			fdConfiguration = init.getFdConfiguration();
			peerConfiguration = init.getMSConfiguration();
			
			idSpaceSize = new BigInteger(2 + "").pow(Scenario1.NUMBER_OF_BITS);
			
			
			int snapshotPeriod = peerConfiguration.getSnapshotPeriod();			
			SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(snapshotPeriod, snapshotPeriod);
			spt.setTimeoutEvent(new GenerateReport(spt));
			trigger(spt, timer);
		}
	};

//-------------------------------------------------------------------	
	Handler<PeerJoin> handlePeerJoin = new Handler<PeerJoin>() {
		public void handle(PeerJoin event) {
			BigInteger id = event.getPeerId();
			
			// join with the next id if this id is taken
			BigInteger successor = view.getNode(id);
			while (successor != null && successor.equals(id)) {
				id = id.add(BigInteger.ONE).mod(idSpaceSize);
				successor = view.getNode(id);
			}
			
			//System.out.println("idSpaceSize: " + idSpaceSize.toString());

			Component newPeer = createAndStartNewPeer(id);
			view.addNode(id);

			trigger(new JoinPeer(id), newPeer.getPositive(PeerPort.class));
		}
	};
	
	//-------------------------------------------------------------------	
	Handler<AllPeerJoin> handleAllPeerJoin = new Handler<AllPeerJoin>() {
		public void handle(AllPeerJoin event) {

			// Read from the Twitter dataset
			readDataset(TWITTER_DATASET_FILENAME);
			
			System.out.println("All Peer Join. size: " + twitterIDs.size());
			
			// assuming that there is no duplicate in the list of node ID
		    Iterator<BigInteger> iter = twitterIDs.iterator();
		    while (iter.hasNext()) {
		    	BigInteger id = iter.next();
				Component newPeer = createAndStartNewPeer(id);
				view.addNode(id);

				trigger(new JoinPeer(id), newPeer.getPositive(PeerPort.class));
		    }
			System.out.println("All Peer Join end");

		}
	};

	
//-------------------------------------------------------------------	
	Handler<PeerFail> handlePeerFail = new Handler<PeerFail>() {
		public void handle(PeerFail event) {
			BigInteger id = view.getNode(event.getPeerId());

			if (view.size() == 0) {
				System.err.println("Empty network");
				return;
			}
			/*
			if (id.equals(Configuration.SOURCE_ID)) {
				System.err.println("Can not remove source ...");
				return;
			}
*/

			view.removeNode(id);
			stopAndDestroyPeer(id);
		}
	};
	
	//-------------------------------------------------------------------	
	Handler<PeerSubscribe> handlePeerSubscribe = new Handler<PeerSubscribe>() {
		public void handle(PeerSubscribe event) {
			BigInteger id = view.getNode(event.getPeerId());
			Component peer = peers.get(id);
//			System.out.println(view + ", id: " + id + ", event id:" + event.getPeerId());

			// selecting a valid random topicID from the list of peers.
			// make sure that it doesn't subscribe to itself
			Random rand = new Random();
			BigInteger tmpBI, topicID;
			do {
				tmpBI = new BigInteger(Scenario1.NUMBER_OF_BITS, rand);
				topicID = view.getNode(tmpBI);
			} while (topicID.equals(id));
			
			
			Positive pos = peer.getPositive(PeerPort.class);
			SubscribePeer sp = new SubscribePeer(topicID);
			
			if (sp == null)
				System.err.println("Event SubscribePeer is null");
			if (pos == null) 
				System.err.println("Port is null");
			
			if (sp != null && pos != null)
				trigger(sp, pos);
		}
	};
	
	// Correlated Subscription Model
	Handler<AllPeerSubscribe_C> handleAllPeerSubscribe_C = new Handler<AllPeerSubscribe_C>() {
		public void handle(AllPeerSubscribe_C event) {
			
			Properties configFile = new Properties();
			try {
				configFile.load(this.getClass().getClassLoader().getResourceAsStream("simulation.properties"));
			} catch (Exception e) {
				System.err.println("Error: couldn't load the properties file in Scenario1.java");
			}
			
			int peersPerBlock = Integer.parseInt(configFile.getProperty("SubscriptionsPerNode"));
			
			determineSubscription(true, peersPerBlock);
		
		}
	};
	
	// Twitter 
	Handler<AllPeerSubscribe_T> handleAllPeerSubscribe_T = new Handler<AllPeerSubscribe_T>() {
		public void handle(AllPeerSubscribe_T event) {

			System.out.println("All Peer Susbcribe");
			// assuming that the twitter dataset was read successfully.
			for (Map.Entry<BigInteger, Set<BigInteger>> entry: subscriptionListForAllNodes.entrySet()) {
			    BigInteger id = entry.getKey();
			    Set<BigInteger> subscriptions = entry.getValue();
			    Component peer = peers.get(id);
			    if(peer!=null)
			    System.out.println("Peer null check");
			   
				

			    // do subscription init
			    SubscriptionInit si = new SubscriptionInit(subscriptions);
				Positive pos = peer.getPositive(PeerPort.class);
				trigger(si, pos);
			}
		
		}
	};
	
	private void readDataset(String fileName) {
		twitterIDs = new HashSet<BigInteger>();
		subscriptionListForAllNodes = new HashMap<BigInteger, Set<BigInteger>>();
		
		int oldSrc = 0;
		int newSrc = 0;
		int subscription = 0;
		boolean firstItr = true;

		Set<BigInteger> subscriptions = new HashSet<BigInteger>();

		try {
			DataInputStream instr = new DataInputStream(
					new BufferedInputStream(new FileInputStream(fileName)));

			// read the first DATASET_LENGTH lines from the twitter dataset
			for (int i = 0; i < DATASET_LENGTH; i++) {
				if (firstItr) {
					oldSrc = instr.readInt();
					newSrc = oldSrc;
					subscription = instr.readInt();
				
					BigInteger bi = absolute(subscription);
					subscriptions.add(bi);
					twitterIDs.add(bi);
					
					twitterIDs.add(absolute(newSrc));
					
					firstItr = false;

					continue;
				}
				
				newSrc = instr.readInt();
				subscription = instr.readInt();

				if (newSrc != oldSrc) {
					subscriptionListForAllNodes.put(absolute(oldSrc), subscriptions);
					twitterIDs.add(absolute(oldSrc));
					
					// preparing for the next node (new source)
					subscriptions = new HashSet<BigInteger>();
					oldSrc = newSrc;
				}
				
				BigInteger bi = absolute(subscription);
				subscriptions.add(bi);
				twitterIDs.add(bi);
			}
			
			if (!subscriptionListForAllNodes.containsKey(absolute(newSrc))){
				subscriptionListForAllNodes.put(absolute(newSrc), subscriptions);
			}
			instr.close();
			
			/*
			// Printing for debugging purpose
			Set<BigInteger> keys = subscriptionListForAllNodes.keySet();
			Iterator<BigInteger> it = keys.iterator();
			for (int j = 0; j < keys.size(); j++) {
				BigInteger o = it.next();
				Set<BigInteger> value = subscriptionListForAllNodes.get(o);
				System.out.println("Key: " + o + ", value: " + value);
			}
			
			System.out.println("twitterIDs: " + twitterIDs);
			*/
			
		} catch (IOException iox) {
			System.out.println("Problem reading " + fileName);
		}
		System.out.println("subscriptionListForAllNodes.size()" +subscriptionListForAllNodes.size());

	}

	private static BigInteger convertPositive(int id) {
		BigInteger tmp;

		id *= -1;
		tmp = BigInteger.valueOf(id);
		tmp = tmp.add(BigInteger.valueOf(Integer.MAX_VALUE));
		return tmp;
	}
	
	private BigInteger absolute(int id) {
		BigInteger result = null;
		
		if (id < 0) {
			result = BigInteger.valueOf(id * -1).add(BigInteger.valueOf(Integer.MAX_VALUE));
		}
		else
			result = BigInteger.valueOf(id);
		
		return result;
	}
	
	//-------------------------------------------------------------------	
	Handler<PeerUnsubscribe> handlePeerUnsubscribe = new Handler<PeerUnsubscribe>() {
		public void handle(PeerUnsubscribe event) {
			// TODO: change the implementation
			//System.err.println("PeerUnsubscribe -- not implemented yet.");
			
			
			BigInteger id = view.getNode(event.getPeerId());
			Component peer = peers.get(id);
			Positive pos = peer.getPositive(PeerPort.class);
			UnsubscribePeer sp = new UnsubscribePeer();
			
			if (sp == null)
				System.err.println("Event UnsubscribePeer is null");
			if (pos == null) 
				System.err.println("Port is null");
			
			if (sp != null && pos != null)
				trigger(sp, pos);
				
		}
	};
	
	//-------------------------------------------------------------------	
	Handler<PeerPublish> handlePeerPublish = new Handler<PeerPublish>() {
		public void handle(PeerPublish event) {
			// TODO: change the implementation
			//System.err.println("PeerPublish -- not implemented yet.");
			
			
			BigInteger id = view.getNode(event.getPeerId());
			Component peer = peers.get(id);
//			System.out.println(view.getTree() + ", id: " + id + ", event id:" + event.getPeerId());
			if (peer == null)
				System.out.println("---------------------");
			Positive pos = peer.getPositive(PeerPort.class);
			PublishPeer sp = new PublishPeer();
			
			if (sp == null)
				System.err.println("Event SubscribePeer is null");
			if (pos == null) 
				System.err.println("Port is null");
			
			if (sp != null && pos != null)
				trigger(sp, pos);
				
		}
	};

//-------------------------------------------------------------------	
	Handler<GenerateReport> handleGenerateReport = new Handler<GenerateReport>() {
		public void handle(GenerateReport event) {
			Snapshot.report();
		}
	};

//-------------------------------------------------------------------	
	private final Component createAndStartNewPeer(BigInteger id) {
		Component peer = create(Peer.class);
		int peerId = ++peerIdSequence;
		Address peerAddress = new Address(peer0Address.getIp(), peer0Address.getPort(), peerId);

		PeerAddress msPeerAddress = new PeerAddress(peerAddress, id);
		
		connect(network, peer.getNegative(Network.class), new MessageDestinationFilter(peerAddress));
		connect(timer, peer.getNegative(Timer.class));
/*
		trigger(new PeerInit(msPeerAddress, serverPeerAddress, peerConfiguration, bootstrapConfiguration, fdConfiguration), 
				peer.getControl());
*/
		trigger(new PeerInit(msPeerAddress, null, peerConfiguration, bootstrapConfiguration, fdConfiguration), 
				peer.getControl());

		trigger(new Start(), peer.getControl());
		peers.put(id, peer);
		peersAddress.put(id, msPeerAddress);
		
		return peer;
	}

	//-------------------------------------------------------------------	
	private final void determineSubscription(boolean isFixed, int peersPerBlock) {
		int numOfBlocks = peers.size() / peersPerBlock;
		
		// give the leftover to the last block
		
		
		this.blocks = new Set[numOfBlocks];
		
		Iterator it = this.peersAddress.entrySet().iterator();
		loop:
		for (int i = 0; i < numOfBlocks; i++) {
			// for each block
			blocks[i] = new HashSet<BigInteger>();
			for (int j = 0; j < peersPerBlock; j++) {
				if (!it.hasNext())
					break loop;
				
				Map.Entry entry = (Map.Entry) it.next();
				BigInteger topicID = (BigInteger) entry.getKey();
				blocks[i].add(topicID);
			}	
		}
		
		// handle the leftover: spread them to the first few blocks
		int count = 0;
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			BigInteger topicID = (BigInteger) entry.getKey();
			
			blocks[count++].add(topicID);			
		}

		Random rand = new Random();
		Iterator it2 = peers.entrySet().iterator();
		while(it2.hasNext()) {
			Map.Entry entry = (Map.Entry) it2.next();
			Component peer = (Component) entry.getValue();
			
			
			SubscriptionInit si = new SubscriptionInit(blocks[rand.nextInt(numOfBlocks)]);
			Positive pos = peer.getPositive(PeerPort.class);
			trigger(si, pos);
		}		
		
	}
//-------------------------------------------------------------------	
	private final void stopAndDestroyPeer(BigInteger id) {
		Component peer = peers.get(id);

		trigger(new Stop(), peer.getControl());
		System.out.println("Triggering STOP event to a peer.");
		

		disconnect(network, peer.getNegative(Network.class));
		disconnect(timer, peer.getNegative(Timer.class));

		Snapshot.removePeer(peersAddress.get(id));

		peers.remove(id);
		peersAddress.remove(id);

		destroy(peer);
	}

//-------------------------------------------------------------------	
	private final static class MessageDestinationFilter extends ChannelFilter<Message, Address> {
		public MessageDestinationFilter(Address address) {
			super(Message.class, address, true);
		}

		public Address getValue(Message event) {
			return event.getDestination();
		}
	}
}

