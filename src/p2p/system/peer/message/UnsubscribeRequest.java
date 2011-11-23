package p2p.system.peer.message;

import java.math.BigInteger;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * 
 * @author Sari Setianingsih
 * @author Jawad Manzoor
 * Created on Oct 1, 2011
 *
 */
public class UnsubscribeRequest extends Message {
	
	private static final long serialVersionUID = -3963584253785169395L;
	private final BigInteger topicID;

	public UnsubscribeRequest(BigInteger topicID, Address src, Address dest) {		
		super( src, dest);
		this.topicID = topicID;
	}

	public BigInteger getTopic() {
		return topicID;
	}

}
