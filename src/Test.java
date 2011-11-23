import java.math.BigInteger;


public class Test {
	
	public static void main(String[] ar) {
		String user = "sari";
		System.out.println("user sari: " + user.hashCode());
		user = "jawad";
		System.out.println("user jawad: " + user.hashCode());
		
		BigInteger bi = BigInteger.valueOf(10);
		BigInteger hashedbi = BigInteger.valueOf(bi.hashCode());
		
		System.out.println("bi: " + bi + " hashedbi: " + hashedbi + " bi.hashCode(): " + bi.hashCode());
		
		
		
	}

}
