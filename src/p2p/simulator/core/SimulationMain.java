
package p2p.simulator.core;


import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import p2p.system.peer.PeerConfiguration;


import se.sics.kompics.ChannelFilter;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.NetworkConfiguration;
import se.sics.kompics.network.model.king.KingLatencyMap;
import se.sics.kompics.p2p.bootstrap.BootstrapConfiguration;
import se.sics.kompics.p2p.bootstrap.server.BootstrapServer;
import se.sics.kompics.p2p.bootstrap.server.BootstrapServerInit;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.p2p.fd.ping.PingFailureDetectorConfiguration;
import se.sics.kompics.p2p.simulator.P2pSimulator;
import se.sics.kompics.p2p.simulator.P2pSimulatorInit;
import se.sics.kompics.simulation.SimulatorScheduler;
import se.sics.kompics.timer.Timer;

public final class SimulationMain extends ComponentDefinition {
	private static SimulatorScheduler simulatorScheduler = new SimulatorScheduler();
	private static SimulationScenario scenario = SimulationScenario.load(System.getProperty("scenario"));

	static {
		PropertyConfigurator.configureAndWatch("log4j.properties");
	}

	
//-------------------------------------------------------------------	
	public static void main(String[] args) {
		Kompics.setScheduler(simulatorScheduler);
		Kompics.createAndStart(SimulationMain.class, 1);
	}
	

//-------------------------------------------------------------------	
	public SimulationMain() throws IOException {
		P2pSimulator.setSimulationPortType(SimulatorPort.class);

		// create
		Component p2pSimulator = create(P2pSimulator.class);
		Component bootstrapServer = create(BootstrapServer.class);
		Component cyclonSimulator = create(Simulator.class);

		// loading component configurations
		final BootstrapConfiguration bootConfiguration = BootstrapConfiguration.load(System.getProperty("bootstrap.configuration"));
		final PeerConfiguration msConfiguration = PeerConfiguration.load(System.getProperty("ms.configuration"));
		final NetworkConfiguration networkConfiguration = NetworkConfiguration.load(System.getProperty("network.configuration"));
		final PingFailureDetectorConfiguration fdConfiguration = PingFailureDetectorConfiguration.load(System.getProperty("ping.fd.configuration"));
		
		trigger(new P2pSimulatorInit(simulatorScheduler, scenario, new KingLatencyMap()), p2pSimulator.getControl());
		trigger(new BootstrapServerInit(bootConfiguration), bootstrapServer.getControl());
		trigger(new SimulatorInit(msConfiguration, bootConfiguration, fdConfiguration, networkConfiguration.getAddress()), cyclonSimulator.getControl());

		final class MessageDestinationFilter extends ChannelFilter<Message, Address> {
			public MessageDestinationFilter(Address address) {
				super(Message.class, address, true);
			}

			public Address getValue(Message event) {
				return event.getDestination();
			}
		}

		// connect
		connect(bootstrapServer.getNegative(Network.class), p2pSimulator.getPositive(Network.class), new MessageDestinationFilter(bootConfiguration.getBootstrapServerAddress()));
		connect(bootstrapServer.getNegative(Timer.class), p2pSimulator.getPositive(Timer.class));
		connect(cyclonSimulator.getNegative(Network.class), p2pSimulator.getPositive(Network.class));
		connect(cyclonSimulator.getNegative(Timer.class), p2pSimulator.getPositive(Timer.class));
		connect(cyclonSimulator.getNegative(SimulatorPort.class), p2pSimulator.getPositive(SimulatorPort.class));
	}
}
