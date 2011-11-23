package p2p.system.peer;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;

public class SendMessage extends Timeout {

//-------------------------------------------------------------------	
	public SendMessage(SchedulePeriodicTimeout request) {
		super(request);
	}
}
