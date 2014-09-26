package org.track.rpc;

import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.util.Hashtable;


import org.track.io.UTF8;
import org.track.io.Writable;

public class Client {

	private Hashtable connections = new Hashtable();
	private Class valueClass;
	private int timeout;
	private int counter;
	private boolean running = true;

	public Client(Class valueClass) {
		this.valueClass = valueClass;
		this.timeout = 10000;
	}

	public void stop() {
		try {
			Thread.sleep(timeout);
		} catch (InterruptedException e) {
		}
		running = false;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public Writable call(Writable param, InetSocketAddress address)
			throws IOException {
		Connection connection = getConnection(address);
		Call call = new Call(param);
		synchronized (call) {
			connection.sendParam(call);
			long wait = timeout;
			do {
				try {
					call.wait(wait);
				} catch (InterruptedException e) {
				}
				wait = timeout
						- (System.currentTimeMillis() - call.lastActivity);
			} while (!call.done && wait > 0);

			if (call.error != null) {
				throw new RemoteException(call.error);
			} else if (!call.done) {
				throw new IOException("timed out waiting for response");
			} else {
				return call.value;
			}
		}
	}

	private Writable makeValue() {
		Writable value;
		try {
			value = (Writable) valueClass.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e.toString());
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e.toString());
		}
		return value;
	}

	private Connection getConnection(InetSocketAddress address)
			throws IOException {
		Connection connection;
		synchronized (connections) {
			connection = (Connection) connections.get(address);
			if (connection == null) {
				connection = new Connection(address);
				connections.put(address, connection);
				connection.start();
			}
		}
		return connection;
	}

	public Writable[] call(Writable[] params, InetSocketAddress[] addresses)
			throws IOException {
		if (addresses.length == 0)
			return new Writable[0];

		ParallelResults results = new ParallelResults(params.length);
		synchronized (results) {
			for (int i = 0; i < params.length; i++) {
				ParallelCall call = new ParallelCall(params[i], results, i);
				try {
					Connection connection = getConnection(addresses[i]);
					connection.sendParam(call);
				} catch (IOException e) {
					results.size--;
				}
			}
			try {
				results.wait(timeout);
			} catch (InterruptedException e) {
			}

			if (results.count == 0) {
				throw new IOException("no responses");
			} else {
				return results.values;
			}
		}
	}

	private class Call {
		int id;
		Writable param;
		Writable value;
		String error;
		long lastActivity;
		boolean done;

		protected Call(Writable param) {
			this.param = param;
			synchronized (Client.this) {
				this.id = counter++;
			}
			touch();
		}

		public synchronized void callComplete() {
			notify();
		}

		public synchronized void touch() {
			lastActivity = System.currentTimeMillis();
		}

		public synchronized void setResult(Writable value, String error) {
			this.value = value;
			this.error = error;
			this.done = true;
		}
	}

	private static class ParallelResults {
		private Writable[] values;
		private int size;
		private int count;

		public ParallelResults(int size) {
			this.values = new Writable[size];
			this.size = size;
		}

		public synchronized void callComplete(ParallelCall call) {
			values[call.index] = call.value;
			count++;
			if (count == size)
				notify();
		}
	}

	private class ParallelCall extends Call {

		private ParallelResults results;
		private int index;

		public ParallelCall(Writable param, ParallelResults results, int index) {
			super(param);
			this.results = results;
			this.index = index;
		}

		public void callComplete() {
			results.callComplete(this);
		}

	}

	private class Connection extends Thread {
		private InetSocketAddress address;
		private Socket socket;
		private DataInputStream in;
		private DataOutputStream out;
		private Hashtable calls = new Hashtable();
		private Call readingCall;
		private Call writingCall;

		public Connection(InetSocketAddress address) throws IOException {
			this.address = address;
			this.socket = new Socket(address.getAddress(), address.getPort());
			socket.setSoTimeout(timeout);
			this.in = new DataInputStream(new BufferedInputStream(
					new FilterInputStream(socket.getInputStream()) {
						public int read(byte[] buf, int off, int len)
								throws IOException {
							int value = super.read(buf, off, len);
							if (readingCall != null) {
								readingCall.touch();
							}
							return value;
						}
					}));
			this.out = new DataOutputStream(new BufferedOutputStream(
					new FilterOutputStream(socket.getOutputStream()) {
						public void write(byte[] buf, int o, int len)
								throws IOException {
							out.write(buf, o, len);
							if (writingCall != null) {
								writingCall.touch();
							}
						}
					}));
			this.setDaemon(true);
			this.setName("Client connection to "
					+ address.getAddress().getHostAddress() + ":"
					+ address.getPort());
		}

		public void run() {
			try {
				while (running) {
					int id;
					try {
						id = in.readInt();
					} catch (SocketTimeoutException e) {
						continue;
					}

					Call call = (Call) calls.remove(new Integer(id));
					boolean isError = in.readBoolean();
					if (isError) {
						UTF8 utf8 = new UTF8();
						utf8.readFields(in);
						call.setResult(null, utf8.toString());
					} else {
						Writable value = makeValue();
						try {
							readingCall = call;
							value.readFields(in);
						} finally {
							readingCall = null;
						}
						call.setResult(value, null);
					}
					call.callComplete();

				}
			} catch (Exception e) {
			} finally {
				close();
			}
		}

		public void close() {
			synchronized (connections) {
				connections.remove(address);
			}
			try {
				socket.close();
			} catch (IOException e) {
			}
		}

		public void sendParam(Call call) throws IOException {
			boolean error = true;
			try {
				calls.put(new Integer(call.id), call);
				synchronized (out) {
					try {
						writingCall = call;
						out.writeInt(call.id);
						call.param.write(out);
						out.flush();
					} finally {
						writingCall = null;
					}
				}
				error = false;
			} finally {
				if (error)
					close();
			}
		}
	}

}
