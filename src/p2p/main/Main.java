package p2p.main;

import p2p.simulator.scenarios.Scenario;
import p2p.simulator.scenarios.Scenario1;

public class Main {
	public static void main(String[] args) throws Throwable {
		Configuration configuration = new Configuration();
		configuration.set();
		
		Scenario scenario = new Scenario1();
		scenario.setSeed(System.currentTimeMillis());
		scenario.simulate();
	}
}
