package p2p.simulator.core;

import java.math.BigInteger;
import java.util.TreeMap;

public class ConsistentHashtable<K> {
	private final TreeMap<K, K> buckets = new TreeMap<K, K>();

//-------------------------------------------------------------------	
	public ConsistentHashtable() {
		super();
	}

//-------------------------------------------------------------------	
	public K addNode(K node) {
		return buckets.put(node, node);
	}

//-------------------------------------------------------------------	
	public K removeNode(K node) {
		return buckets.remove(node);
	}

//-------------------------------------------------------------------	
	public K getNode(K key) {
		if (buckets.isEmpty())
			return null;

		if (!buckets.containsKey(key)) {
			key = buckets.ceilingKey(key);

			if (key == null)
				key = buckets.firstKey();
		}
		
		return buckets.get(key);
	}

//-------------------------------------------------------------------	
	public int size() {
		return buckets.size();
	}

	//-------------------------------------------------------------------	
	public TreeMap<K, K> getTree() {
		return buckets;
	}

	
	
}
