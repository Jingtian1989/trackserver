package org.track.io;

import java.util.HashMap;

public class WritableFactories {
	private static final HashMap CLASSTOFACTORY = new HashMap();

	private WritableFactories() {
	}

	public static synchronized void setFactory(Class c, WritableFactory factory) {
		CLASSTOFACTORY.put(c, factory);
	}

	public static synchronized WritableFactory getFactory(Class c) {
		return (WritableFactory) CLASSTOFACTORY.get(c);
	}

	public static Writable newInstance(Class c) {
		WritableFactory factory = WritableFactories.getFactory(c);
		if (factory != null) {
			return factory.newInstance();
		} else {
			try {
				return (Writable) c.newInstance();
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
