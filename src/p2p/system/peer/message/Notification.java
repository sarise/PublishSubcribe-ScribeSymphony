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
public class Notification extends Message{
	
	private static final long serialVersionUID = -9199390927629685995L;
	private final BigInteger topicID;
	private String content;
	private BigInteger sequenceNum;
	
	public Notification(BigInteger topicID, BigInteger sequenceNum, String content, Address source, Address destination) {		
		super(source, destination);
		this.topicID = topicID;
		this.content = content;
		this.sequenceNum = sequenceNum;
	}

	
	public void setContent(String content) {
		this.content = content;
	}
	
	public BigInteger getSequenceNum() {
		return sequenceNum;
	}

	public BigInteger getTopic() {
		return topicID;
	}

	public String getContent() {
		return content;
	}
	
	
	public String toString() {
		return "{" + topicID + "|" + content + "|" + this.getDestination().toString() + "}";
		
	}
}
