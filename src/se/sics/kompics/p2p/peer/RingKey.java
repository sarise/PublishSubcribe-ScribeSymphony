package se.sics.kompics.p2p.peer;

import java.math.BigInteger;

public final class RingKey {

	public static enum IntervalBounds {
		OPEN_OPEN, OPEN_CLOSED, CLOSED_OPEN, CLOSED_CLOSED;
	}

//-------------------------------------------------------------------
	public static boolean belongsTo(BigInteger id, BigInteger from, BigInteger to, IntervalBounds bounds, BigInteger ringSize) {
		BigInteger ny = ringSize.add(to).subtract(from).mod(ringSize);
		BigInteger nx = ringSize.add(id).subtract(from).mod(ringSize);

		if (bounds.equals(IntervalBounds.OPEN_OPEN)) {
			return ((from.equals(to) && !id.equals(from)) || (nx.compareTo(BigInteger.ZERO) > 0 && nx.compareTo(ny) < 0));
		} else if (bounds.equals(IntervalBounds.OPEN_CLOSED)) {
			return (from.equals(to) || (nx.compareTo(BigInteger.ZERO) > 0 && nx.compareTo(ny) <= 0));
		} else if (bounds.equals(IntervalBounds.CLOSED_OPEN)) {
			return (from.equals(to) || (nx.compareTo(BigInteger.ZERO) >= 0 && nx.compareTo(ny) < 0));
		} else if (bounds.equals(IntervalBounds.CLOSED_CLOSED)) {
			return ((from.equals(to) && id.equals(from)) || (nx.compareTo(BigInteger.ZERO) >= 0 && nx.compareTo(ny) <= 0));
		} else {
			throw new RuntimeException("Unknown interval bounds");
		}
	}	
}
