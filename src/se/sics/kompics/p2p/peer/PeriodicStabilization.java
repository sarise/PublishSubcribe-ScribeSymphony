package se.sics.kompics.p2p.peer;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;

public class PeriodicStabilization extends Timeout {

//-------------------------------------------------------------------	
	public PeriodicStabilization(SchedulePeriodicTimeout request) {
		super(request);
	}
}
