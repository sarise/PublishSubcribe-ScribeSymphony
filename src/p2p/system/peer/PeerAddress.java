package p2p.system.peer;

import java.math.BigInteger;

import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.overlay.OverlayAddress;

public final class PeerAddress extends OverlayAddress implements Comparable<PeerAddress> {

	private static final long serialVersionUID = -7582889514221620065L;
	private final BigInteger peerId;
	//=========
	public static enum IntervalBounds {
		OPEN_OPEN, OPEN_CLOSED, CLOSED_OPEN, CLOSED_CLOSED;
	}
	//==========

//-------------------------------------------------------------------
	public PeerAddress(PeerAddress address) {
		super(address.getPeerAddress());
		this.peerId = address.peerId;
	}

//-------------------------------------------------------------------
	public PeerAddress(Address address, BigInteger peerId) {
		super(address);
		this.peerId = peerId;
	}

//-------------------------------------------------------------------
	public BigInteger getPeerId() {
		return peerId;
	}

//-------------------------------------------------------------------
	public final boolean belongsTo(PeerAddress from, PeerAddress to, IntervalBounds bounds, BigInteger ringSize) {
		BigInteger ny = ringSize.add(to.peerId).subtract(from.peerId).mod(ringSize);
		BigInteger nx = ringSize.add(peerId).subtract(from.peerId).mod(ringSize);

		if (bounds.equals(IntervalBounds.OPEN_OPEN)) {
			return ((from.peerId.equals(to.peerId) && !peerId.equals(from.peerId)) || (nx.compareTo(BigInteger.ZERO) > 0 && nx.compareTo(ny) < 0));
		} else if (bounds.equals(IntervalBounds.OPEN_CLOSED)) {
			return (from.peerId.equals(to.peerId) || (nx.compareTo(BigInteger.ZERO) > 0 && nx.compareTo(ny) <= 0));
		} else if (bounds.equals(IntervalBounds.CLOSED_OPEN)) {
			return (from.peerId.equals(to.peerId) || (nx.compareTo(BigInteger.ZERO) >= 0 && nx.compareTo(ny) < 0));
		} else if (bounds.equals(IntervalBounds.CLOSED_CLOSED)) {
			return ((from.peerId.equals(to.peerId) && peerId.equals(from.peerId)) || (nx.compareTo(BigInteger.ZERO) >= 0 && nx.compareTo(ny) <= 0));
		} else {
			throw new RuntimeException("Unknown interval bounds");
		}
	}
	
//-------------------------------------------------------------------
	@Override
	public int compareTo(PeerAddress that) {
		return peerId.compareTo(that.peerId);
	}

//-------------------------------------------------------------------
	@Override
	public String toString() {
		return peerId.toString();
	}

//-------------------------------------------------------------------
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((peerId == null) ? 0 : peerId.hashCode());
		return result;
	}

//-------------------------------------------------------------------
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		PeerAddress other = (PeerAddress) obj;
		if (peerId == null) {
			if (other.peerId != null)
				return false;
		} else if (!peerId.equals(other.peerId))
			return false;
		return true;
	}

	public int compareByPeerIdTo(int that) {
		// TODO Auto-generated method stub
		return this.peerId.compareTo(BigInteger.valueOf(that));
	}
}
