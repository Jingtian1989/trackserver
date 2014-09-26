package org.track.test;

import java.net.InetSocketAddress;

import org.track.rpc.RPC;

public class Test2 {

	public static void main(String args[]) {
		TestProtocol proxy = (TestProtocol) RPC.getProxy(TestProtocol.class,
				new InetSocketAddress(8090));
		proxy.printf("hello");
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
