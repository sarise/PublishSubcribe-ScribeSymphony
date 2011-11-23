package se.sics.kompics.p2p.peer;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Properties;

public final class PeerConfiguration {

	private final int viewSize;
	private final int snapshotPeriod;

//-------------------------------------------------------------------	
	public PeerConfiguration(int viewSize, int snapshotPeriod) {
		super();
		this.viewSize = viewSize;
		this.snapshotPeriod = snapshotPeriod;
	}

//-------------------------------------------------------------------	
	public int getViewSize() {
		return this.viewSize;
	}

//-------------------------------------------------------------------	
	public int getSnapshotPeriod() {
		return this.snapshotPeriod;
	}

//-------------------------------------------------------------------	
	public void store(String file) throws IOException {
		Properties p = new Properties();
		p.setProperty("viewSize", "" + this.viewSize);
		p.setProperty("snapshotPeriod", "" + this.snapshotPeriod);
		
		Writer writer = new FileWriter(file);
		p.store(writer, "se.sics.kompics.p2p.ms");
	}

//-------------------------------------------------------------------	
	public static PeerConfiguration load(String file) throws IOException {
		Properties p = new Properties();
		Reader reader = new FileReader(file);
		p.load(reader);

		int viewSize = Integer.parseInt(p.getProperty("viewSize"));
		int snapshotPeriod = Integer.parseInt(p.getProperty("snapshotPeriod"));
		
		return new PeerConfiguration(viewSize, snapshotPeriod);
	}
}
