package p2p.system.peer.message;
import java.math.BigInteger;

import p2p.system.peer.PeerAddress;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * 
 * @author Sari Setianingsih
 * @author Jawad Manzoor
 * Created on Oct 1, 2011
 *
 */
public class Publication extends Message{
	

	private static final long serialVersionUID = 6781177817829311117L;
	private final BigInteger topicID;
	private String content;
	private BigInteger sequenceNum;

	
	public Publication(BigInteger topicID, BigInteger seqNum, String content, Address source, Address destination) {		
		super(source, destination);
		this.topicID = topicID;
		this.content = content;
		this.sequenceNum = seqNum;
	}

	public BigInteger getSequenceNum() {
		return sequenceNum;
	}
	
	public void setContent(String content) {
		this.content = content;
	}

	public BigInteger getTopic() {
		return topicID;
	}

	public String getContent() {
		return content;
	}
}
