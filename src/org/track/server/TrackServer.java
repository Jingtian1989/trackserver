package org.track.server;

import java.io.IOException;
import java.net.URL;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class TrackServer {

	private Server server;

	public TrackServer(int port) {
		URL url = TrackServer.class.getClassLoader().getResource("webapps");
		String webapp = url.getPath();
		WebAppContext webctx = new WebAppContext(null, webapp, "/");
		this.server = new Server(port);
		this.server.setHandler(webctx);
	}

	class HTTPStarter implements Runnable {
		@Override
		public void run() {
			try {
				server.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void start() throws IOException {
		new Thread(new HTTPStarter()).start();
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}
		if (!server.isStarted()) {
			throw new IOException("Could not start HTTP server");
		}
	}

	public void stop() {
		try {
			server.stop();
		} catch (Exception e) {
		}
	}

}
