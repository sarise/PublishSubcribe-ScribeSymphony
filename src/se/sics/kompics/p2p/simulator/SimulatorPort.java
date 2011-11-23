package se.sics.kompics.p2p.simulator;

import se.sics.kompics.PortType;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

public class SimulatorPort extends PortType {{
	positive(PeerJoin.class);
	positive(PeerFail.class);	
	negative(TerminateExperiment.class);
}}
