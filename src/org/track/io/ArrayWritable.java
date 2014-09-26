package org.track.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;

public class ArrayWritable implements Writable {

	private Class valueClass;
	private Writable[] values;

	public ArrayWritable() {
		this.valueClass = null;
	}

	public ArrayWritable(Class valueClass) {
		this.valueClass = valueClass;
	}

	public ArrayWritable(Class valueClass, Writable[] values) {
		this(valueClass);
		this.values = values;
	}

	public ArrayWritable(String[] strings) {
		this(UTF8.class, new Writable[strings.length]);
		for (int i = 0; i < strings.length; i++) {
			values[i] = new UTF8(strings[i]);
		}
	}

	public void setValueClass(Class valueClass) {
		if (valueClass != this.valueClass) {
			this.valueClass = valueClass;
			this.values = null;
		}
	}

	public Class getValueClass() {
		return valueClass;
	}

	public String[] toStrings() {
		String[] strings = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			strings[i] = values[i].toString();
		}
		return strings;
	}

	public Object toArray() {
		Object result = Array.newInstance(valueClass, values.length);
		for (int i = 0; i < values.length; i++) {
			Array.set(result, i, values[i]);
		}
		return result;
	}

	public void set(Writable[] values) {
		this.values = values;
	}

	public Writable[] get() {
		return values;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.length);
		for (int i = 0; i < values.length; i++) {
			values[i].write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		values = new Writable[in.readInt()];
		for (int i = 0; i < values.length; i++) {
			Writable value = WritableFactories.newInstance(valueClass);
			values[i] = value;
		}
	}

}
