package org.track.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NullWritable implements Writable {

	private static final NullWritable THIS = new NullWritable();

	private NullWritable() {
	}

	public static NullWritable get() {
		return THIS;
	}

	@Override
	public void write(DataOutput out) throws IOException {

	}

	@Override
	public void readFields(DataInput in) throws IOException {

	}

}
