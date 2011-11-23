package se.sics.kompics.p2p.simulator;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorConfiguration;
import se.sics.kompics.p2p.peer.PeerConfiguration;

public final class SimulatorInit extends Init {

	private final PeerConfiguration msConfiguration;
	private final BootstrapConfiguration bootstrapConfiguration;
	private final PingFailureDetectorConfiguration fdConfiguration;
	
	private final Address peer0Address;

//-------------------------------------------------------------------	
	public SimulatorInit(PeerConfiguration msConfiguration,
			BootstrapConfiguration bootstrapConfiguration,
			PingFailureDetectorConfiguration fdConfiguration,
			Address peer0Address) {
		super();
		this.msConfiguration = msConfiguration;
		this.bootstrapConfiguration = bootstrapConfiguration;
		this.fdConfiguration = fdConfiguration;
		this.peer0Address = peer0Address;
	}

//-------------------------------------------------------------------	
	public PeerConfiguration getMSConfiguration() {
		return msConfiguration;
	}

//-------------------------------------------------------------------	
	public BootstrapConfiguration getBootstrapConfiguration() {
		return bootstrapConfiguration;
	}

//-------------------------------------------------------------------	
	public PingFailureDetectorConfiguration getFdConfiguration() {
		return fdConfiguration;
	}

//-------------------------------------------------------------------	
	public Address getPeer0Address() {
		return peer0Address;
	}
}
