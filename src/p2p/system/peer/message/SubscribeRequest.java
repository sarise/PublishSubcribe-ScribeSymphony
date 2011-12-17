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
public class SubscribeRequest extends Message {
	
	private static final long serialVersionUID = 2876631073644631897L;
	private final BigInteger topicID;
	private final BigInteger lastSequenceNum;
	
	private final int numberOfHops;

	public SubscribeRequest(BigInteger topicID, BigInteger lastSequenceNum, Address src, Address dest, int numberOfHops) {		
		super( src, dest);
		this.topicID = topicID;
		this.lastSequenceNum = lastSequenceNum;
		this.numberOfHops = numberOfHops;
	}

	public BigInteger getTopic() {
		return this.topicID;
	}
	
	public BigInteger getLastSequenceNum() {
		return this.lastSequenceNum;
	}
	
	public int getNumberOfHops() {
		return this.numberOfHops;		
	}
}
