package p2p.simulator.core;

import p2p.simulator.core.event.AllPeerJoin;
import p2p.simulator.core.event.AllPeerSubscribe_T;
import p2p.simulator.core.event.AllPeerSubscribe_C;
import p2p.simulator.core.event.PeerFail;
import p2p.simulator.core.event.PeerJoin;
import p2p.simulator.core.event.PeerPublish;
import p2p.simulator.core.event.PeerSubscribe;
import p2p.simulator.core.event.PeerUnsubscribe;
import p2p.simulator.core.event.ServerStart;
import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public class SimulatorPort extends PortType {{
	positive(PeerJoin.class);
	positive(AllPeerJoin.class);
	positive(ServerStart.class);
	positive(PeerFail.class);	
	positive(PeerSubscribe.class);
	positive(PeerUnsubscribe.class);
	positive(PeerPublish.class);
	positive(AllPeerSubscribe_T.class);
	positive(AllPeerSubscribe_C.class);
	negative(TerminateExperiment.class);
}}
