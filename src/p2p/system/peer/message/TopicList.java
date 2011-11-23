package p2p.system.peer.message;

import java.math.BigInteger;
import java.util.Random;

public class TopicList {
	
	public static BigInteger[] list = {BigInteger.ZERO, BigInteger.ONE, BigInteger.valueOf(2), BigInteger.valueOf(3)};
	
	public static BigInteger getRandomTopic() {
		Random rand = new Random();
		return list[rand.nextInt(list.length)];
	}

}
