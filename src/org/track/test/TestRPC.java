package org.track.test;

import java.io.IOException;

import org.track.rpc.RPC;
import org.track.rpc.Server;

public class TestRPC implements TestProtocol {

	private Server server;

	public TestRPC() {
		this.server = RPC.getServer(this, 8090);
	}

	public void offerService() throws IOException, InterruptedException {
		this.server.start();
		this.server.join();
	}

	public void printf(String str) {
		System.out.println(str);
	}

}
