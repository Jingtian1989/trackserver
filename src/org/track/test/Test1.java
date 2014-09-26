package org.track.test;

import java.io.IOException;

public class Test1 {

	public static void main(String args[]) {
		TestRPC testRPC = new TestRPC();
		try {
			testRPC.offerService();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
