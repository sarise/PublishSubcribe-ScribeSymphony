package p2p.system.peer;

import java.math.BigInteger;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import p2p.simulator.scenarios.Scenario1;
import p2p.system.peer.event.JoinPeer;
import p2p.system.peer.event.PublishPeer;
import p2p.system.peer.event.SubscribePeer;
import p2p.system.peer.event.SubscriptionInit;
import p2p.system.peer.event.UnsubscribePeer;
import p2p.system.peer.message.ForwardingTable;
import p2p.system.peer.message.Notification;
import p2p.system.peer.message.Publication;
import p2p.system.peer.message.SubscribeRequest;
import p2p.system.peer.message.TopicList;
import p2p.system.peer.message.UnsubscribeRequest;

//import centralized.simulator.snapshot.Snapshot;

import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.bootstrap.BootstrapCompleted;
import se.sics.kompics.p2p.bootstrap.BootstrapRequest;
import se.sics.kompics.p2p.bootstrap.BootstrapResponse;
import se.sics.kompics.p2p.bootstrap.P2pBootstrap;
import se.sics.kompics.p2p.bootstrap.PeerEntry;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClient;
import se.sics.kompics.p2p.bootstrap.client.BootstrapClientInit;
import se.sics.kompics.p2p.fd.FailureDetector;
import se.sics.kompics.p2p.fd.PeerFailureSuspicion;
import se.sics.kompics.p2p.fd.StartProbingPeer;
import se.sics.kompics.p2p.fd.StopProbingPeer;
import se.sics.kompics.p2p.fd.SuspicionStatus;
import se.sics.kompics.p2p.fd.ping.PingFailureDetector;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorInit;
import se.sics.kompics.p2p.peer.FindSuccReply;
import se.sics.kompics.p2p.peer.Notify;
import se.sics.kompics.p2p.peer.PeriodicStabilization;
import se.sics.kompics.p2p.peer.RingKey;
import se.sics.kompics.p2p.peer.WhoIsPred;
import se.sics.kompics.p2p.peer.WhoIsPredReply;
import se.sics.kompics.p2p.simulator.launch.Configuration;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

import se.sics.kompics.p2p.peer.FindSucc;
import se.sics.kompics.p2p.simulator.snapshot.Snapshot;
import se.sics.kompics.p2p.simulator.snapshot.PeerInfo;

public final class Peer extends ComponentDefinition {

	public static BigInteger RING_SIZE = new BigInteger(2 + "")
			.pow(Configuration.Log2Ring);
	public static int FINGER_SIZE = Configuration.Log2Ring;
	public static int SUCC_SIZE = Configuration.Log2Ring; // WOW! a peer has
															// backup succ as
															// much as the
															// finger size??
	private static int WAIT_TIME_TO_REJOIN = 15;
	private static int WAIT_TIME_TO_REPLICATE = 3;
	private static int STABILIZING_PERIOD = 1000;
	private PeerAddress pred;
	private PeerAddress succ;
	private PeerAddress[] fingers = new PeerAddress[FINGER_SIZE];
	private PeerAddress[] succList = new PeerAddress[SUCC_SIZE];
	int count = 0;

	private int fingerIndex = 0;
	private int joinCounter = 0;
	private int replicateCounter = 0;
	private boolean started = false;

	// ======================

	Negative<PeerPort> msPeerPort = negative(PeerPort.class);

	Positive<Network> network = positive(Network.class);
	Positive<Timer> timer = positive(Timer.class);

	private Component fd, bootstrap;

	private Random rand;
	private Address myAddress;
	private Address serverAddress;
	private PeerAddress myPeerAddress;
	private PeerAddress serverPeerAddress;
	private Vector<PeerAddress> friends;
	private int msgPeriod;
	private int viewSize;
	private boolean bootstrapped;

	private HashMap<Address, UUID> fdRequests;
	private HashMap<Address, PeerAddress> fdPeers;

	private HashMap<BigInteger, BigInteger> mySubscriptions; // <Topic ID, last
																// sequence
																// number>
	private HashMap<BigInteger, Vector<Publication>> eventRepository; // <Topic
																		// ID,
																		// list
																		// of
																		// Notification>
	private HashMap<BigInteger, Set<Address>> myForwardingTable; // <Topic ID,
																	// list of
																	// PeerAddress
																	// (your
	private HashMap<BigInteger, Set<Address>> predForwardingTable;
	private BigInteger publicationSeqNum;

	// -------------------------------------------------------------------
	public Peer() {

		// ====================
		for (int i = 0; i < SUCC_SIZE; i++)
			this.succList[i] = null;

		for (int i = 0; i < FINGER_SIZE; i++)
			this.fingers[i] = null;

		// =========================
		fdRequests = new HashMap<Address, UUID>();
		fdPeers = new HashMap<Address, PeerAddress>();
		rand = new Random(System.currentTimeMillis());
		mySubscriptions = new HashMap<BigInteger, BigInteger>();
		eventRepository = new HashMap<BigInteger, Vector<Publication>>();
		myForwardingTable = new HashMap<BigInteger, Set<Address>>();
		predForwardingTable = new HashMap<BigInteger, Set<Address>>();

		fd = create(PingFailureDetector.class);
		bootstrap = create(BootstrapClient.class);

		publicationSeqNum = BigInteger.ONE;

		connect(network, fd.getNegative(Network.class));
		connect(network, bootstrap.getNegative(Network.class));
		connect(timer, fd.getNegative(Timer.class));
		connect(timer, bootstrap.getNegative(Timer.class));

		subscribe(handleInit, control);
		subscribe(handleStart, control);

		// subscribe(handleSendMessage, timer);
		// subscribe(handleRecvMessage, network);
		subscribe(handleJoin, msPeerPort);
		subscribe(handleSubscribe, msPeerPort);
		subscribe(handleUnsubscribe, msPeerPort);
		subscribe(handlePublish, msPeerPort);

		subscribe(handleSubscriptionInit, msPeerPort);

		subscribe(handleBootstrapResponse,
				bootstrap.getPositive(P2pBootstrap.class));
		subscribe(handlePeerFailureSuspicion,
				fd.getPositive(FailureDetector.class));

		subscribe(eventPublicationHandler, network);
		subscribe(eventNotificationHandler, network);
		subscribe(subscribeHandler, network);
		subscribe(unsubscribeHandler, network);
		subscribe(forwardingTableHandler, network);

		// =============
		subscribe(handlePeriodicStabilization, timer);
		subscribe(handleFindSucc, network);
		subscribe(handleFindSuccReply, network);
		subscribe(handleWhoIsPred, network);
		subscribe(handleWhoIsPredReply, network);
		subscribe(handleNotify, network);

		// =================
		// subscribe(messageHandler, network);
	}

	// -------------------------------------------------------------------
	// This handler initiates the Peer component.
	// -------------------------------------------------------------------
	Handler<PeerInit> handleInit = new Handler<PeerInit>() {
		@Override
		public void handle(PeerInit init) {

			myPeerAddress = init.getMSPeerSelf();
			myAddress = myPeerAddress.getPeerAddress();
			serverPeerAddress = null; // init.getServerPeerAddress();
			serverAddress = null;// serverPeerAddress.getPeerAddress(); //TODO:
									// remove server
			friends = new Vector<PeerAddress>();
			msgPeriod = init.getMSConfiguration().getSnapshotPeriod();

			viewSize = init.getMSConfiguration().getViewSize();

			trigger(new BootstrapClientInit(myAddress,
					init.getBootstrapConfiguration()), bootstrap.getControl());
			trigger(new PingFailureDetectorInit(myAddress,
					init.getFdConfiguration()), fd.getControl());

			System.out.println("Peer " + myPeerAddress.getPeerId()
					+ " is initialized.");
		}
	};

	// -------------------------------------------------------------------
	// Whenever a new node joins the system, this handler is triggered
	// by the simulator.
	// In this method the node sends a request to the bootstrap server
	// to get a pre-defined number of existing nodes.
	// You can change the number of requested nodes through peerConfiguration
	// defined in Configuration.java.
	// Here, the node adds itself to the Snapshot.
	// -------------------------------------------------------------------
	Handler<JoinPeer> handleJoin = new Handler<JoinPeer>() {
		@Override
		public void handle(JoinPeer event) {
			Snapshot.addPeer(myPeerAddress);
			BootstrapRequest request = new BootstrapRequest("chord", viewSize); // ("chord",1)
			trigger(request, bootstrap.getPositive(P2pBootstrap.class));
		}
	};

	// -------------------------------------------------------------------
	// Whenever a node receives a response from the bootstrap server
	// this handler is triggered.
	// In this handler, the nodes adds the received list to its friend
	// list and registers them in the failure detector.
	// In addition, it sets a periodic scheduler to call the
	// SendMessage handler periodically.
	// -------------------------------------------------------------------

	Handler<BootstrapResponse> handleBootstrapResponse = new Handler<BootstrapResponse>() {

		@Override
		public void handle(BootstrapResponse event) {

			if (!bootstrapped) {
				bootstrapped = true;
				PeerAddress peer;
				Set<PeerEntry> somePeers = event.getPeers();
				// System.out.println("Peer "+myPeerAddress
				// +" in bootstrap response "+ count++ +" somePeers: "+somePeers
				// );

				/*
				 * for (PeerEntry peerEntry : somePeers) { peer =
				 * (PeerAddress)peerEntry.getOverlayAddress();
				 * friends.addElement(peer); fdRegister(peer); }
				 * 
				 * trigger(new BootstrapCompleted("Lab0", myPeerAddress),
				 * bootstrap.getPositive(P2pBootstrap.class));
				 * Snapshot.addFriends(myPeerAddress, friends);
				 * 
				 * SchedulePeriodicTimeout spt = new
				 * SchedulePeriodicTimeout(msgPeriod, msgPeriod);
				 * spt.setTimeoutEvent(new SendMessage(spt)); trigger(spt,
				 * timer);
				 */

				if (somePeers.size() == 0) {
					pred = null;
					succ = myPeerAddress;
					succList[0] = succ;
					Snapshot.setPred(myPeerAddress, pred);
					Snapshot.setSucc(myPeerAddress, succ);
					joinCounter = -1;
					trigger(new BootstrapCompleted("chord", myPeerAddress),
							bootstrap.getPositive(P2pBootstrap.class));
				} else {
					pred = null;
					PeerAddress existingPeer = (PeerAddress) somePeers
							.iterator().next().getOverlayAddress();
					trigger(new FindSucc(myPeerAddress, existingPeer,
							myPeerAddress, myPeerAddress.getPeerId(), 0),
							network);
					Snapshot.setPred(myPeerAddress, pred);
				}

				if (!started) {
					SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(
							STABILIZING_PERIOD, STABILIZING_PERIOD);
					spt.setTimeoutEvent(new PeriodicStabilization(spt));
					trigger(spt, timer);
					started = true;
				}
			}
		}
	};

	// System.out.println("Peer subscribed to initHandler, startHandler, and eventNotificationHandler.");

	// --------------------chord

	// -------------------------------------------------------------------
	Handler<FindSucc> handleFindSucc = new Handler<FindSucc>() {
		public void handle(FindSucc event) {
			BigInteger id = event.getID();
			PeerAddress initiator = event.getInitiator();
			int fingerIndex = event.getFingerIndex();

			if (succ != null
					&& RingKey.belongsTo(id, myPeerAddress.getPeerId(),
							succ.getPeerId(),
							RingKey.IntervalBounds.OPEN_CLOSED, RING_SIZE))
				trigger(new FindSuccReply(myPeerAddress, initiator, succ,
						fingerIndex), network);
			else {
				PeerAddress nextPeer = closestPrecedingNode(id);
				trigger(new FindSucc(myPeerAddress, nextPeer, initiator, id,
						fingerIndex), network);
			}
		}
	};

	// -------------------------------------------------------------------
	Handler<FindSuccReply> handleFindSuccReply = new Handler<FindSuccReply>() {
		public void handle(FindSuccReply event) {
			PeerAddress responsible = event.getResponsible();
			int fingerIndex = event.getFingerIndex();

			if (fingerIndex == 0) {
				succ = new PeerAddress(responsible);
				succList[0] = new PeerAddress(responsible);
				Snapshot.setSucc(myPeerAddress, succ);
				trigger(new BootstrapCompleted("chord", myPeerAddress),
						bootstrap.getPositive(P2pBootstrap.class));
				fdRegister(succ);
				joinCounter = -1;
			}

			fingers[fingerIndex] = new PeerAddress(responsible);
			Snapshot.setFingers(myPeerAddress, fingers);
		}
	};

	// -------------------------------------------------------------------
	Handler<PeriodicStabilization> handlePeriodicStabilization = new Handler<PeriodicStabilization>() {
		public void handle(PeriodicStabilization event) {
			// System.out.println("*********************");
			if (succ == null && joinCounter != -1) { // means we haven't joined
														// the ring yet
				if (joinCounter++ > Peer.WAIT_TIME_TO_REJOIN) { // waited
																// enough, time
																// to retransmit
																// my request
					joinCounter = 0;
					bootstrapped = false;

					BootstrapRequest request = new BootstrapRequest("chord", 1);
					trigger(request, bootstrap.getPositive(P2pBootstrap.class));
				}
			}

			if (succ != null)
				trigger(new WhoIsPred(myPeerAddress, succ), network);

			// fix fingers
			if (succ == null)
				return;

			fingerIndex++;
			if (fingerIndex == FINGER_SIZE)
				fingerIndex = 1;

			BigInteger index = new BigInteger(2 + "").pow(fingerIndex);
			BigInteger id = myPeerAddress.getPeerId().add(index).mod(RING_SIZE);

			if (RingKey.belongsTo(id, myPeerAddress.getPeerId(),
					succ.getPeerId(), RingKey.IntervalBounds.OPEN_CLOSED,
					RING_SIZE))
				fingers[fingerIndex] = new PeerAddress(succ);
			else {
				PeerAddress nextPeer = closestPrecedingNode(id);
				trigger(new FindSucc(myPeerAddress, nextPeer, myPeerAddress,
						id, fingerIndex), network);
			}

			// Send updated forwardingTable to succ periodically
			/*
			if (replicateCounter++ > WAIT_TIME_TO_REPLICATE) {
				replicateCounter = 0;
				ForwardingTable table = new ForwardingTable(
						getForwardingTable(), myAddress, succ.getPeerAddress());
				trigger(table, network);
			}
			*/
			
			/*
			 * System.out.print("Fingers for peer " + myAddress.getId() + ": ");
			 * for (int i = 0; i < fingers.length; i++) if (fingers[i] != null)
			 * System.out.print(fingers[i].getPeerId() + " ");
			 * System.out.println();
			 */
		}
	};

	// -------------------------------------------------------------------
	Handler<WhoIsPred> handleWhoIsPred = new Handler<WhoIsPred>() {
		public void handle(WhoIsPred event) {
			PeerAddress requester = event.getMSPeerSource();
			trigger(new WhoIsPredReply(myPeerAddress, requester, pred, succList),
					network);
		}
	};

	// -------------------------------------------------------------------
	Handler<WhoIsPredReply> handleWhoIsPredReply = new Handler<WhoIsPredReply>() {
		public void handle(WhoIsPredReply event) {
			PeerAddress succPred = event.getPred();
			PeerAddress[] succSuccList = event.getSuccList();

			if (succ == null)
				return;

			if (succPred != null) {
				if (RingKey.belongsTo(succPred.getPeerId(),
						myPeerAddress.getPeerId(), succ.getPeerId(),
						RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE)) {
					succ = new PeerAddress(succPred);
					fingers[0] = succ;
					succList[0] = succ;
					Snapshot.setSucc(myPeerAddress, succ);
					Snapshot.setFingers(myPeerAddress, fingers);
					fdRegister(succ);
					joinCounter = -1;
				}
			}

			for (int i = 1; i < succSuccList.length; i++) {
				if (succSuccList[i - 1] != null)
					succList[i] = new PeerAddress(succSuccList[i - 1]);
			}

			Snapshot.setSuccList(myPeerAddress, succList);

			if (succ != null)
				trigger(new Notify(myPeerAddress, succ, myPeerAddress), network);
		}
	};

	// -------------------------------------------------------------------
	Handler<Notify> handleNotify = new Handler<Notify>() {
		public void handle(Notify event) {
			PeerAddress newPred = event.getID();

			if (pred == null
					|| RingKey.belongsTo(newPred.getPeerId(), pred.getPeerId(),
							myPeerAddress.getPeerId(),
							RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE)) {
				pred = new PeerAddress(newPred);
				fdRegister(pred);
				Snapshot.setPred(myPeerAddress, newPred);
			}
		}
	};

	// -------------------------------------------------------------------
	// If a node has registered for another node, e.g. P, this handler
	// is triggered if P fails.
	// -------------------------------------------------------------------
	Handler<PeerFailureSuspicion> handlePeerFailureSuspicion = new Handler<PeerFailureSuspicion>() {
		@Override
		public void handle(PeerFailureSuspicion event) {
			Address suspectedPeerAddress = event.getPeerAddress();

			if (event.getSuspicionStatus().equals(SuspicionStatus.SUSPECTED)) {
				if (!fdPeers.containsKey(suspectedPeerAddress)
						|| !fdRequests.containsKey(suspectedPeerAddress))
					return;

				PeerAddress suspectedPeer = fdPeers.get(suspectedPeerAddress);
				fdUnregister(suspectedPeer);
				if (suspectedPeer.equals(pred)) {
					pred = null;
					// If pred is dead then i am responsible for his range also
					// Therefore merge my forwarding table with pred forwarding
					// table
					/*
					System.out
							.println("-------------- MY TABLE ----------------");
					Set<BigInteger> keys = myForwardingTable.keySet();
					Iterator<BigInteger> it = keys.iterator();
					for (int j = 0; j < keys.size(); j++) {
						BigInteger o = it.next();
						System.out.println("Key: " + o + ", set: "
								+ myForwardingTable.get(o));
					}

					keys = predForwardingTable.keySet();
					it = keys.iterator();

					System.out
							.println("-------------- PRED TABLE ----------------");
					for (int j = 0; j < keys.size(); j++) {
						BigInteger o = it.next();
						System.out.println("Key: " + o + ", set: "
								+ predForwardingTable.get(o));
					}

					keys = predForwardingTable.keySet();
					it = keys.iterator();
			
					for (int j = 0; j < keys.size(); j++) {
						BigInteger index = it.next();
				
						Set<Address> values = new HashSet<Address>();

						if (myForwardingTable.containsKey(index)) {
							values = myForwardingTable.get(index);
							values.addAll(predForwardingTable.get(index));
							myForwardingTable.put(index, values);
						} else {
							myForwardingTable.put(index,
									predForwardingTable.get(index));
						}
					}
					// myForwardingTable.putAll(predForwardingTable); 
					//doesn't work. replaces the value in my table if the same key exists in pred table

					keys = myForwardingTable.keySet();
					it = keys.iterator();
					System.out
							.println("-------------- MERGED TABLE ----------------");
					for (int j = 0; j < keys.size(); j++) {
						BigInteger o = it.next();
						System.out.println("Key: " + o + ", set: "
								+ myForwardingTable.get(o));
					}
					// maybe predForwardingTable.clear();
					 */
				}
				

				if (suspectedPeer.equals(succ)) {
					int i;
					for (i = 1; i < Peer.SUCC_SIZE; i++) {
						if (succList[i] != null
								&& !succList[i].equals(myPeerAddress)
								&& !succList[i].equals(suspectedPeer)) {
							succ = succList[i];
							fingers[0] = succ;
							fdRegister(succ);
							// Handling replication
							// When successor changes, send my own forwarding
							// table to the new succ
							/*
							ForwardingTable table = new ForwardingTable(
									getForwardingTable(), myAddress,
									succ.getPeerAddress());
							trigger(table, network);
							*/
							break;
						} else
							succ = null;
					}

					joinCounter = 0;

					Snapshot.setSucc(myPeerAddress, succ);
					Snapshot.setFingers(myPeerAddress, fingers);

					for (; i > 0; i--)
						succList = leftshift(succList);
				}

				for (int i = 1; i < Peer.SUCC_SIZE; i++) {
					if (succList[i] != null
							&& succList[i].equals(suspectedPeer))
						succList[i] = null;
				}

				/*
				 * friends.removeElement(suspectedPeer);
				 * System.out.println(myPeerAddress + " detects failure of " +
				 * suspectedPeer); }
				 */
			}
		}
	};

	public HashMap<BigInteger, Set<Address>> getForwardingTable() {
		return myForwardingTable;
	}

	Handler<ForwardingTable> forwardingTableHandler = new Handler<ForwardingTable>() {
		@Override
		public void handle(ForwardingTable msg) {

			// System.out.println(" forwardingTableHandler called "+myAddress);
			predForwardingTable = new HashMap<BigInteger, Set<Address>>();
			predForwardingTable = msg.getForwardingTable();

		}
	};

	// -------------------------------------------------------------------
	private PeerAddress closestPrecedingNode(BigInteger id) {
		for (int i = FINGER_SIZE - 1; i >= 0; i--) {
			if (fingers[i] != null
					&& RingKey.belongsTo(fingers[i].getPeerId(),
							myPeerAddress.getPeerId(), id,
							RingKey.IntervalBounds.OPEN_OPEN, RING_SIZE))
				return fingers[i];
		}

		return myPeerAddress;
	}

	// -------------------------------------------------------------------
	private PeerAddress[] leftshift(PeerAddress[] list) {
		PeerAddress[] newList = new PeerAddress[list.length];

		for (int i = 1; i < list.length; i++)
			newList[i - 1] = list[i];

		newList[list.length - 1] = null;

		return newList;
	}

	Handler<Publication> eventPublicationHandler = new Handler<Publication>() {
		public void handle(Publication publication) {
			// EVENT REPOSITORY
			BigInteger hashedTopicID = hashFunction(publication.getTopic());

			if (between(hashedTopicID, pred.getPeerId(),
					myPeerAddress.getPeerId())) {
				// I am the rendezvous node

				System.out.println("*** $ peer " + myPeerAddress.getPeerId()
						+ " is the rendezvous node for topicID:"
						+ publication.getTopic());

				// Add the publication to the EventRepository according to
				// topicID
				Vector<Publication> eventList = eventRepository.get(publication
						.getTopic());
				if (!eventRepository.containsKey(publication.getTopic())) {
					eventList = new Vector<Publication>();
				}
				eventList.add(publication);

				// Forward the corresponding notification based on the
				// forwardingTable
				Notification notification = new Notification(
						publication.getTopic(), publication.getSequenceNum(),
						publication.getContent(), myAddress, null);
				forwardNotification(notification);
			} else {
				// I am not the rendezvous node

				// should I store this publication? although I am not the
				// rendezvous node.

				// route this publication to the rendezvous node
				Publication newPublication = new Publication(
						publication.getTopic(), publication.getSequenceNum(),
						publication.getContent(), publication.getSource(),
						publication.getDestination());
				routeMessage(newPublication, hashedTopicID);
			}
		}
	};

	private void forwardNotification(Notification msg) {
		// Forward the corresponding notification based on the forwardingTable
		Set<Address> subscriberlist = myForwardingTable.get(msg.getTopic());

		if (subscriberlist == null) {
			System.out.println("No subscriber in the forwarding table");
			return;
		}

		Iterator<Address> itr = subscriberlist.iterator();
		while (itr.hasNext()) {
			Address nextHop = itr.next();

			Notification notification = new Notification(msg.getTopic(),
					msg.getSequenceNum(), msg.getContent(), msg.getSource(),
					nextHop);

			trigger(notification, network);
		}
	}

	Handler<Notification> eventNotificationHandler = new Handler<Notification>() {
		@Override
		public void handle(Notification msg) {

			// Check whether I am also the subscriber for that topicID
			if (mySubscriptions.containsKey(msg.getTopic())) {
				System.out.println("# Peer " + myPeerAddress.getPeerId()
						+ ", as a subscriber, received a notification about "
						+ msg.getTopic());
				Snapshot.receiveNotification(msg.getTopic(), myPeerAddress, msg.getSequenceNum());
			} else {
				System.out
						.println("Peer "
								+ myPeerAddress.getPeerId()
								+ " , as a forwarder only, received a notification about "
								+ msg.getTopic());
			}

			// Forward the notification using the forwardingTable
			forwardNotification(msg);

		}
	};

	Handler<UnsubscribeRequest> unsubscribeHandler = new Handler<UnsubscribeRequest>() {
		public void handle(UnsubscribeRequest msg) {
			//
			System.out.println("- Peer " + myPeerAddress.getPeerId()
					+ " received an UnsubcribeRequest.");

			UnsubscribeRequest newMsg = new UnsubscribeRequest(msg.getTopic(),
					myAddress, null);
			BigInteger hashedTopicID = hashFunction(msg.getTopic());

			Set<Address> subscriberlist = myForwardingTable.get(newMsg
					.getTopic());
			if (subscriberlist == null) {
				System.out.println("No entry in the forwarding table.");
				routeMessage(newMsg, hashedTopicID);
			} else {
				subscriberlist.remove(msg.getSource());
				if (subscriberlist.isEmpty()) {
					System.out.println("No more subscribers.");
					myForwardingTable.remove(newMsg.getTopic());
					routeMessage(newMsg, hashedTopicID);
				} else {
					myForwardingTable.put(newMsg.getTopic(), subscriberlist);
					System.out
							.println("Not forwarding the UnsubscribeRequest. subscriberlist: "
									+ subscriberlist.toString());
				}
			}
		}
	};

	Handler<SubscribeRequest> subscribeHandler = new Handler<SubscribeRequest>() {
		public void handle(SubscribeRequest msg) {
			//
			// System.out.println("\n+ Peer " + myPeerAddress.getPeerId() +
			// " received a SubcribeRequest.");

			// TODO: lastSequenceNum, should I check with the lastSequenceNum
			// with respect to the forwarding table.
			SubscribeRequest newMsg = new SubscribeRequest(msg.getTopic(),
					msg.getLastSequenceNum(), myAddress, null);

			Set<Address> tmp = myForwardingTable.get(newMsg.getTopic());
			if (tmp == null) {
				tmp = new HashSet<Address>();
			}
			tmp.add(msg.getSource());
			myForwardingTable.put(newMsg.getTopic(), tmp);

			SubscribeRequest msg2 = new SubscribeRequest(newMsg.getTopic(),
					newMsg.getLastSequenceNum(), newMsg.getSource(), null);

			BigInteger hashedTopicID = hashFunction(msg.getTopic());

			// System.out.println("id: " + myPeerAddress.getPeerId() +
			// " destination: " + hashedTopicID + " topicID: " +
			// msg.getTopic());

			routeMessage(msg2, hashedTopicID);
			// routeMessage(msg2, msg.getTopic());
		}
	};

	// Helper methods

	private boolean between(BigInteger destID, BigInteger predID,
			BigInteger myID) {
		if (destID.equals(myID))
			return true;

		if (predID.compareTo(myID) == 1)
			myID = myID.add(RING_SIZE);

		if (destID.compareTo(myID) == -1 && destID.compareTo(predID) == 1)
			return true;
		else
			return false;
	}

	private BigInteger hashFunction(BigInteger bi) {
		BigInteger result;

		int hashCode = bi.toString().hashCode();
		result = BigInteger.valueOf(hashCode);

		if (hashCode < 0) {
			result = result.abs().add(BigInteger.valueOf(Integer.MAX_VALUE));
		}

		return result.mod(RING_SIZE);

	}

	private void routeMessage(Message msg, BigInteger destination) {
		BigInteger oldDistance = RING_SIZE;
		BigInteger newDistance = RING_SIZE;
		Address address = null;
		BigInteger nextPeer = BigInteger.ZERO;

		// System.out.println("id: " + myPeerAddress.getPeerId() +
		// " destination: " + destination);

		if (pred != null
				&& between(destination, pred.getPeerId(),
						myPeerAddress.getPeerId())) {
			// I am the rendezvous node
			System.out.println("***");// Peer " + myPeerAddress.getPeerId() + "
										// is the rendezvous node for " +
										// destination);
		} else if (succ != null
				&& between(destination, myPeerAddress.getPeerId(),
						succ.getPeerId())) {
			// The rendezvous node is the successor
			address = succ.getPeerAddress();
		} else {
			// I am not the rendezvous node, route the message to the rendezvous
			// node

			BigInteger nextHopID = null;
			if (pred == null)
				System.err.println("Peer " + myPeerAddress.getPeerId()
						+ ": pred is null.");

			// first, check in the succ list
			if (succ != null) {
				// System.out.println("succ: " + succ.getPeerId() +
				// " destination: " + destination);

				nextHopID = succ.getPeerId();
				// peerID < topic =: finger ------ dest
				if (nextHopID.compareTo(destination) == -1
						|| nextHopID.compareTo(destination) == 0) {
					newDistance = destination.subtract(nextHopID);
				}
				// peerID > topic =: finger --- max --- dest
				else {
					newDistance = RING_SIZE.subtract(nextHopID);
					newDistance = newDistance.add(destination);// destination.subtract(nextHopID).add(RING_SIZE);
					// System.out.println("RING_SIZE: " + RING_SIZE + " xxx " +
					// BigInteger.valueOf(Integer.MAX_VALUE).multiply(BigInteger.valueOf(2)));
				}
			} else
				System.err.println("succ is null");

			// System.out.println("nextHopID: " + nextHopID + ", distance: " +
			// newDistance);

			// newDistance < oldDisntace
			if (newDistance.compareTo(oldDistance) == -1) {
				oldDistance = newDistance;
				address = succ.getPeerAddress();
				nextPeer = succ.getPeerId();
			}

			// then, check in the fingers list
			for (int i = 0; i < fingers.length; i++) {

				if (newDistance.equals(BigInteger.ZERO))
					break;

				if (fingers[i] != null) {

					// System.out.println("fingers: " + fingers[i].getPeerId() +
					// " destination: " + destination
					// + " oldDistance " + oldDistance);

					// peerID < topic =: finger ------ dest
					nextHopID = fingers[i].getPeerId();
					if (nextHopID.compareTo(destination) == -1
							|| nextHopID.compareTo(destination) == 0) {
						newDistance = destination.subtract(nextHopID);
					}
					// peerID > topic =: finger --- max --- dest
					else {
						// newDistance =
						// destination.subtract(fingers[i].getPeerId()).add(RING_SIZE);
						newDistance = RING_SIZE.subtract(nextHopID);
						newDistance = newDistance.add(destination);// destination.subtract(nextHopID).add(RING_SIZE);
					}
				}

				// System.out.println("nextHopID: " + nextHopID + ", distance: "
				// + newDistance);

				// newDistance < oldDisntace
				if (newDistance.compareTo(oldDistance) == -1) {
					// System.out.println("newDistance: " + newDistance +
					// ", oldDistance:" + oldDistance);
					oldDistance = newDistance;
					address = fingers[i].getPeerAddress();
					nextPeer = fingers[i].getPeerId();
				}
			}
		}
		// System.out.println("oldDistance:" + oldDistance);

		if (address != null) {
			System.out.println("Peer " + myPeerAddress.getPeerId()
					+ " routed a message on id " + nextPeer + " " + address);
			msg.setDestination(address);
			trigger(msg, network);
		}

		// else
		// System.err.println("Message is dropped.");

	}

	// -------------------------------------------------------------------------
	private void sendSubscribeRequest(BigInteger topicID,
			BigInteger lastSequenceNum) {

		BigInteger hashedTopicID = hashFunction(topicID);
		SubscribeRequest sub = new SubscribeRequest(topicID, lastSequenceNum,
				myAddress, null);

		Snapshot.addSubscription(topicID, myPeerAddress, lastSequenceNum);
		System.out.println("+ Peer " + myPeerAddress.getPeerId()
				+ " is triggering a SubscribeRequest topicID: " + topicID
				+ " hashed: " + hashedTopicID);

		routeMessage(sub, hashedTopicID);
		// routeMessage(sub, topicID);
	}

	private void sendUnsubscribeRequest(BigInteger topicID) {
		BigInteger hashedTopicID = hashFunction(topicID);
		UnsubscribeRequest unsub = new UnsubscribeRequest(topicID, myAddress,
				null);

		Snapshot.removeSubscription(topicID, myPeerAddress);
		System.out.println("- Peer " + myPeerAddress.getPeerId()
				+ " is triggering a UnsubscribeRequest topicID: " + topicID
				+ " hashed: " + hashedTopicID);

		routeMessage(unsub, hashedTopicID);
	}

	private void publish(BigInteger topicID, String content) {
		System.out.println("\nPeer " + myPeerAddress.getPeerId()
				+ " is publishing an event.");

		Publication publication = new Publication(topicID, publicationSeqNum,
				content, myAddress, null);

		BigInteger hashedTopicID = hashFunction(topicID);

		// The publisher is the rendezvous itself
		// This should not be the ideal case.

		if (pred != null
				&& between(hashedTopicID, pred.getPeerId(),
						myPeerAddress.getPeerId())) {
			// I am the rendezvous node
			// Stop routing the publication
			// And then, start to forward the corresponding notification based
			// on the forwardingTable
			System.out.println("$ I am the rendezvous node.");
			Notification notification = new Notification(
					publication.getTopic(), publication.getSequenceNum(),
					publication.getContent(), myAddress, null);
			forwardNotification(notification);
		} else {
			System.out.println("$ Route the message.");
			routeMessage(publication, hashedTopicID);
		}

		Snapshot.publish(myPeerAddress, publicationSeqNum);
		publicationSeqNum.add(BigInteger.ONE);
	}

	Handler<Start> handleStart = new Handler<Start>() {
		@Override
		public void handle(Start event) {
			// System.out.println("Peer -- inside the handleStart()");
			/*
			 * System.out.println("Peer " + myAddress.getId() + " is started.");
			 * Address add = new Address(myAddress.getIp(), myAddress.getPort(),
			 * myAddress.getId()-1); Notification notification = new
			 * Notification("test", "nothing", myAddress, myAddress);
			 * trigger(notification, network); String topic = "Football";
			 * sendSubscribeRequest(topic);
			 */

			// sendUnsubscribeRequest(topic);
		}
	};

	Handler<SubscriptionInit> handleSubscriptionInit = new Handler<SubscriptionInit>() {
		@Override
		public void handle(SubscriptionInit si) {
			Set<BigInteger> topicIDs = si.getTopicIDs();

			Iterator it = topicIDs.iterator();
			while (it.hasNext()) {
				BigInteger topicID = (BigInteger) it.next();
				sendSubscribeRequest(topicID, BigInteger.ZERO);

			}

		}
	};

	Handler<SubscribePeer> handleSubscribe = new Handler<SubscribePeer>() {
		@Override
		public void handle(SubscribePeer event) {
			BigInteger topicID = event.getTopicID();

			BigInteger lastSequenceNumber = BigInteger.ZERO;
			if (mySubscriptions.containsKey(topicID))
				lastSequenceNumber = mySubscriptions.get(topicID);
			mySubscriptions.put(topicID, lastSequenceNumber);

			sendSubscribeRequest(topicID, lastSequenceNumber);
		}
	};

	Handler<UnsubscribePeer> handleUnsubscribe = new Handler<UnsubscribePeer>() {
		@Override
		public void handle(UnsubscribePeer event) {

			System.out.println("Peer " + myPeerAddress.getPeerId()
					+ " is unsubscribing an event.");

			if (!mySubscriptions.isEmpty()) {
				Set<BigInteger> topicIDs = mySubscriptions.keySet(); // TODO: we
																		// can
																		// randomize
																		// later.
																		// randomization
																		// should
																		// be
																		// done
																		// in
																		// the
																		// simulation
																		// class.
				Iterator<BigInteger> it = topicIDs.iterator();
				BigInteger topicID = it.next();

				// topicID should not be removed from the list, so that the next
				// subscription can use the lastSequenceNumber
				// mySubscriptions.remove(topicID);

				sendUnsubscribeRequest(topicID);
			}
		}
	};

	Handler<PublishPeer> handlePublish = new Handler<PublishPeer>() {
		@Override
		public void handle(PublishPeer event) {
			String info = "Test";
			// publish(TopicList.getRandomTopic(), info); // Assumptions: we can
			// publish something that we don't subscribe

			publish(myPeerAddress.getPeerId(), info);
		}
	};

	// -------------------------------------------------------------------
	// This handler is called periodically, every msgPeriod milliseconds.
	// -------------------------------------------------------------------
	/*
	 * Handler<SendMessage> handleSendMessage = new Handler<SendMessage>() {
	 * 
	 * @Override public void handle(SendMessage event) { sendMessage(); } };
	 */
	// -------------------------------------------------------------------
	// Whenever a node receives a PeerMessage from another node, this
	// handler is triggered.
	// In this handler the node, add the address of the sender and the
	// address of another nodes, which has been sent by PeerMessage
	// to its friend list, and updates its state in the Snapshot.
	// The node registers the nodes added to its friend list and
	// unregisters the node removed from the list.
	// -------------------------------------------------------------------
	/*
	 * Handler<PeerMessage> handleRecvMessage = new Handler<PeerMessage>() {
	 * 
	 * @Override public void handle(PeerMessage event) { PeerAddress oldFriend;
	 * PeerAddress sender = event.getMSPeerSource(); PeerAddress newFriend =
	 * event.getNewFriend();
	 * 
	 * // add the sender address to the list of friends if
	 * (!friends.contains(sender)) { if (friends.size() == viewSize) { oldFriend
	 * = friends.get(rand.nextInt(viewSize)); friends.remove(oldFriend);
	 * fdUnregister(oldFriend); Snapshot.removeFriend(myPeerAddress, oldFriend);
	 * }
	 * 
	 * friends.addElement(sender); fdRegister(sender);
	 * Snapshot.addFriend(myPeerAddress, sender); }
	 * 
	 * // add the received new friend from the sender to the list of friends if
	 * (!friends.contains(newFriend) && !myPeerAddress.equals(newFriend)) { if
	 * (friends.size() == viewSize) { oldFriend =
	 * friends.get(rand.nextInt(viewSize)); friends.remove(oldFriend);
	 * fdUnregister(oldFriend); Snapshot.removeFriend(myPeerAddress, oldFriend);
	 * }
	 * 
	 * friends.addElement(newFriend); fdRegister(newFriend);
	 * Snapshot.addFriend(myPeerAddress, newFriend); } } };
	 */
	// -------------------------------------------------------------------
	// In this method a node selects a random node, e.g. randomDest,
	// and sends it the address of another random node from its friend
	// list, e.g. randomFriend.
	// -------------------------------------------------------------------
	/*
	 * private void sendMessage() { if (friends.size() == 0) return;
	 * 
	 * PeerAddress randomDest = friends.get(rand.nextInt(friends.size()));
	 * PeerAddress randomFriend = friends.get(rand.nextInt(friends.size()));
	 * 
	 * if (randomFriend != null) trigger(new PeerMessage(myPeerAddress,
	 * randomDest, randomFriend), network); }
	 */
	// -------------------------------------------------------------------
	// This method shows how to register the failure detector for a node.
	// -------------------------------------------------------------------
	private void fdRegister(PeerAddress peer) {
		Address peerAddress = peer.getPeerAddress();
		StartProbingPeer spp = new StartProbingPeer(peerAddress, peer);
		fdRequests.put(peerAddress, spp.getRequestId());
		trigger(spp, fd.getPositive(FailureDetector.class));

		fdPeers.put(peerAddress, peer);
	}

	// -------------------------------------------------------------------
	// This method shows how to unregister the failure detector for a node.
	// -------------------------------------------------------------------
	private void fdUnregister(PeerAddress peer) {
		if (peer == null)
			return;

		Address peerAddress = peer.getPeerAddress();
		trigger(new StopProbingPeer(peerAddress, fdRequests.get(peerAddress)),
				fd.getPositive(FailureDetector.class));
		fdRequests.remove(peerAddress);

		fdPeers.remove(peerAddress);
	}
}
