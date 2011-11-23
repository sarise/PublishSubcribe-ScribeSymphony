package p2p.simulator.scenarios;


import java.util.Random;

import p2p.simulator.core.SimulationMain;


import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

public class Scenario {
	private static Random random;
	protected SimulationScenario scenario;

//-------------------------------------------------------------------
	public Scenario(SimulationScenario scenario) {
		this.scenario = scenario;
		this.scenario.setSeed(System.currentTimeMillis());
		random = scenario.getRandom();
	}

//-------------------------------------------------------------------
	public void setSeed(long seed) {
		this.scenario.setSeed(seed);
	}

//-------------------------------------------------------------------
	public void simulate() {
		this.scenario.simulate(SimulationMain.class);
	}

//-------------------------------------------------------------------
	public static Random getRandom() {
		return random;
	}

//-------------------------------------------------------------------
	public static void setRandom(Random r) {
		random = r;
	}
}
