package org.track.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

import org.track.io.ObjectWritable;
import org.track.io.UTF8;
import org.track.io.Writable;

public class RPC {

	private static Client CLIENT;

	private static class Invocation implements Writable {
		private String methodName;
		private Class[] parameterClasses;
		private Object[] parameters;

		public Invocation() {
		}

		public Invocation(Method method, Object[] parameters) {
			this.methodName = method.getName();
			this.parameterClasses = method.getParameterTypes();
			this.parameters = parameters;
		}

		public String getMethodName() {
			return methodName;
		}

		public Class[] getParameterClasses() {
			return parameterClasses;
		}

		public Object[] getParameters() {
			return parameters;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			UTF8.writeString(out, methodName);
			out.writeInt(parameterClasses.length);
			for (int i = 0; i < parameterClasses.length; i++) {
				ObjectWritable.writeObject(out, parameters[i],
						parameterClasses[i]);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			methodName = UTF8.readString(in);
			parameters = new Object[in.readInt()];
			parameterClasses = new Class[parameters.length];
			ObjectWritable objectWritable = new ObjectWritable();
			for (int i = 0; i < parameters.length; i++) {
				parameters[i] = ObjectWritable.readObject(in, objectWritable);
				parameterClasses[i] = objectWritable.getDeclaredClass();
			}
		}

		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append(methodName);
			buffer.append("(");
			for (int i = 0; i < parameters.length; i++) {
				if (i != 0 || i != (parameters.length - 1))
					buffer.append(", ");
				buffer.append(parameters[i]);
			}
			buffer.append(")");
			return buffer.toString();
		}

	}

	private static class Invoker implements InvocationHandler {
		private InetSocketAddress address;

		public Invoker(InetSocketAddress address) {
			this.address = address;
			if (CLIENT == null) {
				CLIENT = new Client(ObjectWritable.class);
			}

		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			ObjectWritable value = (ObjectWritable) CLIENT.call(new Invocation(
					method, args), address);
			return value.get();
		}
	}

	public static class Server extends org.track.rpc.Server {
		private Object instance;
		private Class implementation;

		public Server(Object instance, int port) {
			this(instance, port, 1);
		}

		public Server(Object instance, int port, int numHandlers) {
			super(port, Invocation.class, numHandlers);
			this.instance = instance;
			this.implementation = instance.getClass();
		}

		public Writable call(Writable param) throws IOException {
			try {
				Invocation call = (Invocation) param;

				Method method = implementation.getMethod(call.getMethodName(),
						call.getParameterClasses());

				Object value = method.invoke(instance, call.getParameters());
				return new ObjectWritable(method.getReturnType(), value);

			} catch (InvocationTargetException e) {
				Throwable target = e.getTargetException();
				if (target instanceof IOException) {
					throw (IOException) target;
				} else {
					IOException ioe = new IOException(target.toString());
					ioe.setStackTrace(target.getStackTrace());
					throw ioe;
				}
			} catch (Throwable e) {
				IOException ioe = new IOException(e.toString());
				ioe.setStackTrace(e.getStackTrace());
				throw ioe;
			}
		}
	}

	public static Object getProxy(Class protocol, InetSocketAddress address) {
		return Proxy.newProxyInstance(protocol.getClassLoader(),
				new Class[] { protocol }, new Invoker(address));
	}

	public static Object[] call(Method method, Object[][] params,
			InetSocketAddress[] address) throws IOException {

		Invocation[] invocations = new Invocation[params.length];
		for (int i = 0; i < params.length; i++)
			invocations[i] = new Invocation(method, params[i]);
		if (CLIENT == null) {
			CLIENT = new Client(ObjectWritable.class);
		}
		Writable[] wrappedValues = CLIENT.call(invocations, address);

		if (method.getReturnType() == Void.TYPE) {
			return null;
		}

		Object[] values = (Object[]) Array.newInstance(method.getReturnType(),
				wrappedValues.length);
		for (int i = 0; i < values.length; i++)
			if (wrappedValues[i] != null)
				values[i] = ((ObjectWritable) wrappedValues[i]).get();

		return values;
	}

	public static Server getServer(final Object instance, final int port) {
		return getServer(instance, port, 1, false);
	}

	public static Server getServer(final Object instance, final int port,
			final int numHandlers, final boolean verbose) {
		return new Server(instance, port, numHandlers);
	}

}
