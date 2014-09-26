package org.track.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.LinkedList;

import org.track.io.UTF8;
import org.track.io.Writable;

public abstract class Server {

	private static final ThreadLocal SERVER = new ThreadLocal();

	private int port;
	private int handlerCount;
	private int maxQueuedCalls;
	private Class paramClass;
	private int timeout;
	private boolean running = true;
	private LinkedList callQueue = new LinkedList();
	private Object callDequeued = new Object();

	private static class Call {
		private int id;
		private Writable param;
		private Connection connection;

		public Call(int id, Writable param, Connection connection) {
			this.id = id;
			this.param = param;
			this.connection = connection;
		}
	}

	private class Listener extends Thread {
		private ServerSocket socket;

		public Listener() throws IOException {
			this.socket = new ServerSocket(port);
			socket.setSoTimeout(timeout);
			this.setDaemon(true);
			this.setName("Server listener on port " + port);
		}

		public void run() {
			while (running) {
				try {
					new Connection(socket.accept()).start();
				} catch (SocketTimeoutException e) {
				} catch (Exception e) {
				}
			}
			try {
				socket.close();
			} catch (IOException e) {
			}
		}
	}

	private class Connection extends Thread {
		private Socket socket;
		private DataInputStream in;
		private DataOutputStream out;

		public Connection(Socket socket) throws IOException {
			this.socket = socket;
			socket.setSoTimeout(timeout);
			this.in = new DataInputStream(new BufferedInputStream(
					socket.getInputStream()));
			this.out = new DataOutputStream(new BufferedOutputStream(
					socket.getOutputStream()));
			this.setDaemon(true);
			this.setName("Server connection on port " + port + " from "
					+ socket.getInetAddress().getHostAddress());
		}

		public void run() {
			SERVER.set(Server.this);
			try {
				while (running) {
					int id;
					try {
						id = in.readInt();
					} catch (SocketTimeoutException e) {
						continue;
					}
					Writable param = makeParam();
					param.readFields(in);
					Call call = new Call(id, param, this);

					synchronized (callQueue) {
						callQueue.addLast(call);
						callQueue.notify();
					}
					while (running && callQueue.size() >= maxQueuedCalls) {
						synchronized (callDequeued) {
							callDequeued.wait(timeout);

						}
					}
				}
			} catch (EOFException e) {
			} catch (SocketException e) {
			} catch (Exception e) {
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private class Handler extends Thread {
		public Handler(int instanceNumber) {
			this.setDaemon(true);
			this.setName("Server handler " + instanceNumber + " on " + port);
		}

		public void run() {
			SERVER.set(Server.this);
			while (running) {
				try {
					Call call = null;
					synchronized (callQueue) {
						while (running && callQueue.size() == 0) {
							callQueue.wait(timeout);
						}
						if (!running)
							break;
						call = (Call) callQueue.removeFirst();
					}
					synchronized (callDequeued) {
						callDequeued.notify();
					}
					String error = null;
					Writable value = null;
					try {
						value = call(call.param);
					} catch (IOException e) {
						error = getStackTrace(e);
					} catch (Exception e) {
						error = getStackTrace(e);
					}

					DataOutputStream out = call.connection.out;
					synchronized (out) {
						out.writeInt(call.id);
						out.writeBoolean(error != null);
						if (error != null)
							value = new UTF8(error);
						value.write(out);
						out.flush();
					}
				} catch (Exception e) {
				}
			}
		}

		private String getStackTrace(Throwable throwable) {
			StringWriter stringWriter = new StringWriter();
			PrintWriter printWriter = new PrintWriter(stringWriter);
			throwable.printStackTrace(printWriter);
			printWriter.flush();
			return stringWriter.toString();
		}
	}

	public abstract Writable call(Writable param) throws IOException;

	public static Server get() {
		return (Server) SERVER.get();
	}

	protected Server(int port, Class paramClass, int handlerCount) {
		this.port = port;
		this.paramClass = paramClass;
		this.handlerCount = handlerCount;
		this.maxQueuedCalls = handlerCount;
		this.timeout = 10000;
	}

	public synchronized void start() throws IOException {
		Listener listener = new Listener();
		listener.start();

		for (int i = 0; i < handlerCount; i++) {
			Handler handler = new Handler(i);
			handler.start();
		}
	}

	public synchronized void stop() {
		running = false;
		try {
			Thread.sleep(timeout);
		} catch (InterruptedException e) {
		}
		notifyAll();
	}

	public synchronized void join() throws InterruptedException {
		while (running) {
			wait();
		}
	}

	private Writable makeParam() {
		Writable param;
		try {
			param = (Writable) paramClass.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e.toString());
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e.toString());
		}
		return param;
	}

}
