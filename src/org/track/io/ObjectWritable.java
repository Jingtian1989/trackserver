package org.track.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

public class ObjectWritable implements Writable {

	private static final Map PRIMITIVENAMES = new HashMap();
	private Class declaredClass;
	private Object instance;

	static {
		PRIMITIVENAMES.put("boolean", Boolean.TYPE);
		PRIMITIVENAMES.put("byte", Byte.TYPE);
		PRIMITIVENAMES.put("char", Character.TYPE);
		PRIMITIVENAMES.put("short", Short.TYPE);
		PRIMITIVENAMES.put("int", Integer.TYPE);
		PRIMITIVENAMES.put("long", Long.TYPE);
		PRIMITIVENAMES.put("float", Float.TYPE);
		PRIMITIVENAMES.put("double", Double.TYPE);
		PRIMITIVENAMES.put("void", Void.TYPE);
	}

	public ObjectWritable() {
	}

	public ObjectWritable(Class declaredClass, Object instance) {
		this.declaredClass = declaredClass;
		this.instance = instance;
	}

	public Object get() {
		return instance;
	}

	public Class getDeclaredClass() {
		return declaredClass;
	}

	public void set(Object instance) {
		this.declaredClass = instance.getClass();
		this.instance = instance;
	}

	public void readFields(DataInput in) throws IOException {
		readObject(in);
	}

	public void write(DataOutput out) throws IOException {
		writeObject(out, instance, declaredClass);
	}

	public static void writeObject(DataOutput out, Object instance,
			Class declaredClass) throws IOException {

		if (instance == null) {
			instance = new NullInstance(declaredClass);
			declaredClass = NullInstance.class;
		}
		if (instance instanceof Writable) {
			UTF8.writeString(out, instance.getClass().getName());
			((Writable) instance).write(out);
			return;
		}
		UTF8.writeString(out, declaredClass.getName());

		if (declaredClass.isArray()) {
			int length = Array.getLength(instance);
			out.writeInt(length);
			for (int i = 0; i < length; i++) {
				writeObject(out, Array.get(instance, i),
						declaredClass.getComponentType());
			}
		} else if (declaredClass == String.class) {
			UTF8.writeString(out, (String) instance);
		} else if (declaredClass.isPrimitive()) {
			if (declaredClass == Boolean.TYPE) {
				out.writeBoolean(((Boolean) instance).booleanValue());
			} else if (declaredClass == Character.TYPE) {
				out.writeChar(((Character) instance).charValue());
			} else if (declaredClass == Byte.TYPE) {
				out.writeByte(((Byte) instance).byteValue());
			} else if (declaredClass == Short.TYPE) {
				out.writeShort(((Short) instance).shortValue());
			} else if (declaredClass == Integer.TYPE) {
			} else if (declaredClass == Long.TYPE) {
				out.writeLong(((Long) instance).longValue());
			} else if (declaredClass == Float.TYPE) {
				out.writeFloat(((Float) instance).floatValue());
			} else if (declaredClass == Double.TYPE) {
				out.writeDouble(((Double) instance).doubleValue());
			} else if (declaredClass == Void.TYPE) {
			} else {
				throw new IllegalArgumentException("Not a primitive: "
						+ declaredClass);
			}
		}
	}

	public static Object readObject(DataInput in) throws IOException {
		return readObject(in, null);
	}

	public static Object readObject(DataInput in, ObjectWritable objectWritable)
			throws IOException {
		Object instance;
		String className = UTF8.readString(in);
		Class declaredClass = (Class) PRIMITIVENAMES.get(className);

		if (declaredClass == null) {
			try {
				declaredClass = Class.forName(className);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e.toString());
			}
		}

		if (declaredClass == NullInstance.class) {
			NullInstance wrapper = new NullInstance();
			wrapper.readFields(in);
			declaredClass = wrapper.declaredClass;
			instance = null;

		} else if (declaredClass.isPrimitive()) {

			if (declaredClass == Boolean.TYPE) {
				instance = Boolean.valueOf(in.readBoolean());
			} else if (declaredClass == Character.TYPE) {
				instance = new Character(in.readChar());
			} else if (declaredClass == Byte.TYPE) {
				instance = new Byte(in.readByte());
			} else if (declaredClass == Short.TYPE) {
				instance = new Short(in.readShort());
			} else if (declaredClass == Integer.TYPE) {
				instance = new Integer(in.readInt());
			} else if (declaredClass == Long.TYPE) {
				instance = new Long(in.readLong());
			} else if (declaredClass == Float.TYPE) {
				instance = new Float(in.readFloat());
			} else if (declaredClass == Double.TYPE) {
				instance = new Double(in.readDouble());
			} else if (declaredClass == Void.TYPE) {
				instance = null;
			} else {
				throw new IllegalArgumentException("Not a primitive: "
						+ declaredClass);
			}

		} else if (declaredClass.isArray()) {
			int length = in.readInt();
			instance = Array.newInstance(declaredClass.getComponentType(),
					length);
			for (int i = 0; i < length; i++) {
				Array.set(instance, i, readObject(in));
			}

		} else if (declaredClass == String.class) {
			instance = UTF8.readString(in);

		} else { // Writable
			Writable writable = WritableFactories.newInstance(declaredClass);
			writable.readFields(in);
			instance = writable;
		}

		if (objectWritable != null) {
			objectWritable.declaredClass = declaredClass;
			objectWritable.instance = instance;
		}

		return instance;

	}

	private static class NullInstance implements Writable {
		private Class declaredClass;

		public NullInstance() {
		}

		public NullInstance(Class declaredClass) {
			this.declaredClass = declaredClass;
		}

		public void readFields(DataInput in) throws IOException {
			String className = UTF8.readString(in);
			declaredClass = (Class) PRIMITIVENAMES.get(className);
			if (declaredClass == null) {
				try {
					declaredClass = Class.forName(className);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException();
				}
			}
		}

		public void write(DataOutput out) throws IOException {
			UTF8.writeString(out, declaredClass.getName());
		}
	}

}
