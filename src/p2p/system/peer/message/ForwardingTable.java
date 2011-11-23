package p2p.system.peer.message;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Set;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * 
 * @author Sari Setianingsih
 * @author Jawad Manzoor
 * Created on Nov 21, 2011
 *
 */
public class ForwardingTable extends Message{
	
	private static final long serialVersionUID = -9199390927629685995L;


	private HashMap<BigInteger, Set<Address>> forwardingTable;
	
	public ForwardingTable(HashMap<BigInteger, Set<Address>> forwardingTable, Address source, Address destination) {		
		super(source, destination);
		this.forwardingTable = forwardingTable;	
	}
	
	public HashMap<BigInteger, Set<Address>> getForwardingTable() {
		return forwardingTable;
	}
	
}
