package p2p.simulator.scenarios;


import java.math.BigInteger;

import p2p.simulator.core.event.AllPeerJoin;
import p2p.simulator.core.event.AllPeerSubscribe;
import p2p.simulator.core.event.AllPeerSubscribe_T;
import p2p.simulator.core.event.AllPeerSubscribe_C;
import p2p.simulator.core.event.PeerFail;
import p2p.simulator.core.event.PeerJoin;
import p2p.simulator.core.event.PeerPublish;
import p2p.simulator.core.event.PeerSubscribe;
import p2p.simulator.core.event.PeerUnsubscribe;
import p2p.simulator.core.event.ServerStart;


import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation1;

@SuppressWarnings("serial")
public class Operations {

//-------------------------------------------------------------------
	static Operation1<PeerJoin, BigInteger> peerJoin = new Operation1<PeerJoin, BigInteger>() {
			public PeerJoin generate(BigInteger id) {
				return new PeerJoin(id);
			}
		};


//-------------------------------------------------------------------
	static Operation<AllPeerJoin> allPeerJoin = new Operation<AllPeerJoin>() {
			public AllPeerJoin generate() {
				return new AllPeerJoin();
			}
		};

		
//-------------------------------------------------------------------
	static Operation1<PeerFail, BigInteger> peerFail = new Operation1<PeerFail, BigInteger>() {
		public PeerFail generate(BigInteger id) {
			return new PeerFail(id);
		}
	};
	
//-------------------------------------------------------------------
	static Operation1<ServerStart, BigInteger> serverStart = new Operation1<ServerStart, BigInteger>() {
			public ServerStart generate(BigInteger id) {
				return new ServerStart(id);
			}
		};
		
//-------------------------------------------------------------------
	static Operation1<PeerSubscribe, BigInteger> peerSubscribe = new Operation1<PeerSubscribe, BigInteger>() {
			public PeerSubscribe generate(BigInteger id) {
				return new PeerSubscribe(id);
			}
		};
		
//-------------------------------------------------------------------
	static Operation<AllPeerSubscribe_C> allPeerSubscribe_C = new Operation<AllPeerSubscribe_C>() {
			public AllPeerSubscribe_C generate() {
				return new AllPeerSubscribe_C();
			}
		};
		
//-------------------------------------------------------------------
		static Operation<AllPeerSubscribe_T> allPeerSubscribe_T = new Operation<AllPeerSubscribe_T>() {
				public AllPeerSubscribe_T generate() {
					return new AllPeerSubscribe_T();
				}
			};
		
//-------------------------------------------------------------------
	static Operation1<PeerUnsubscribe, BigInteger> peerUnsubscribe = new Operation1<PeerUnsubscribe, BigInteger>() {
			public PeerUnsubscribe generate(BigInteger id) {
				return new PeerUnsubscribe(id);
			}
		};
		
//-------------------------------------------------------------------
	static Operation1<PeerPublish, BigInteger> peerPublish = new Operation1<PeerPublish, BigInteger>() {
			public PeerPublish generate(BigInteger id) {
				return new PeerPublish(id);
			}
		};
}
