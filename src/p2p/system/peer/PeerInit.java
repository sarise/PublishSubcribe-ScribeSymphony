package p2p.system.peer;

import se.sics.kompics.Init;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorConfiguration;

public final class PeerInit extends Init {

	private final PeerAddress msPeerSelf;
	private final PeerAddress serverPeerAddress;
	private final BootstrapConfiguration bootstrapConfiguration;
	private final PeerConfiguration msConfiguration;
	private final PingFailureDetectorConfiguration fdConfiguration;
	
	private final int networkSize;

//-------------------------------------------------------------------	
	public PeerInit(PeerAddress msPeerSelf,
			PeerAddress serverPeerAddress,
			PeerConfiguration msConfiguration,
			BootstrapConfiguration bootstrapConfiguration,
			PingFailureDetectorConfiguration fdConfiguration,
			int networkSize) {
		super();
		this.msPeerSelf = msPeerSelf;
		this.serverPeerAddress = serverPeerAddress;
		this.bootstrapConfiguration = bootstrapConfiguration;
		this.msConfiguration = msConfiguration;
		this.fdConfiguration = fdConfiguration;
		this.networkSize = networkSize;
	}

//-------------------------------------------------------------------	
	public PeerAddress getMSPeerSelf() {
		return msPeerSelf;
	}
	
//-------------------------------------------------------------------	
	public PeerAddress getServerPeerAddress() {
		return serverPeerAddress;
	}

//-------------------------------------------------------------------	
	public BootstrapConfiguration getBootstrapConfiguration() {
		return bootstrapConfiguration;
	}

//-------------------------------------------------------------------	
	public PeerConfiguration getMSConfiguration() {
		return msConfiguration; 
	}
	
//-------------------------------------------------------------------	
	public PingFailureDetectorConfiguration getFdConfiguration() {
		return fdConfiguration;
	}
	
//-------------------------------------------------------------------	
	public int getNetworkSize(){
		return this.networkSize;
	}
}
