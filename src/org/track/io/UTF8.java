package org.track.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UTF8 implements WritableComparable {

	private static final DataOutputBuffer OUTPUTBUFFER = new DataOutputBuffer();
	private static final DataInputBuffer INPUTBUFFER = new DataInputBuffer();
	private static final byte[] EMPTYBYTES = new byte[0];
	private byte[] bytes = EMPTYBYTES;
	private int length;

	public UTF8() {
	}

	public UTF8(String string) {
		set(string);
	}

	public UTF8(UTF8 utf8) {
		set(utf8);
	}

	public byte[] getBytes() {
		return bytes;
	}

	public int getLength() {
		return length;
	}

	public void set(String string) {
		if (string.length() > 0xffff / 3) {
			string = string.substring(0, 0xffff / 3);
		}

		length = utf8Length(string);
		if (length > 0xffff)
			throw new RuntimeException("string too long!");

		if (bytes == null || length > bytes.length)
			bytes = new byte[length];

		try {
			synchronized (OUTPUTBUFFER) {
				OUTPUTBUFFER.reset();
				writeChars(OUTPUTBUFFER, string, 0, string.length());
				System.arraycopy(OUTPUTBUFFER.getData(), 0, bytes, 0, length);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void set(UTF8 other) {
		length = other.length;
		if (bytes == null || length > bytes.length) // grow buffer
			bytes = new byte[length];
		System.arraycopy(other.bytes, 0, bytes, 0, length);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(length);
		out.write(bytes, 0, length);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		length = in.readUnsignedShort();
		if (bytes == null || bytes.length < length)
			bytes = new byte[length];
		in.readFully(bytes, 0, length);
	}

	public static void skip(DataInput in) throws IOException {
		int length = in.readUnsignedShort();
		in.skipBytes(length);
	}

	public int compareTo(Object o) {
		UTF8 that = (UTF8) o;
		return WritableComparator.compareBytes(bytes, 0, length, that.bytes, 0,
				that.length);
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer(length);
		try {
			synchronized (INPUTBUFFER) {
				INPUTBUFFER.reset(bytes, length);
				readChars(INPUTBUFFER, buffer, length);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return buffer.toString();
	}

	public boolean equals(Object o) {
		if (!(o instanceof UTF8))
			return false;
		UTF8 that = (UTF8) o;
		if (this.length != that.length)
			return false;
		else
			return WritableComparator.compareBytes(bytes, 0, length,
					that.bytes, 0, that.length) == 0;
	}

	public int hashCode() {
		return WritableComparator.hashBytes(bytes, length);
	}

	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(UTF8.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int n1 = readUnsignedShort(b1, s1);
			int n2 = readUnsignedShort(b2, s2);
			return compareBytes(b1, s1 + 2, n1, b2, s2 + 2, n2);
		}
	}

	static {
		WritableComparator.define(UTF8.class, new Comparator());
	}

	public static byte[] getBytes(String string) {
		byte[] result = new byte[utf8Length(string)];
		try { // avoid sync'd allocations
			synchronized (OUTPUTBUFFER) {
				OUTPUTBUFFER.reset();
				writeChars(OUTPUTBUFFER, string, 0, string.length());
				System.arraycopy(OUTPUTBUFFER.getData(), 0, result, 0,
						OUTPUTBUFFER.getLength());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return result;
	}

	public static String readString(DataInput in) throws IOException {
		int bytes = in.readUnsignedShort();
		StringBuffer buffer = new StringBuffer(bytes);
		readChars(in, buffer, bytes);
		return buffer.toString();
	}

	private static void readChars(DataInput in, StringBuffer buffer, int nBytes)
			throws IOException {
		synchronized (OUTPUTBUFFER) {
			OUTPUTBUFFER.reset();
			OUTPUTBUFFER.write(in, nBytes);
			byte[] bytes = OUTPUTBUFFER.getData();
			int i = 0;
			while (i < nBytes) {
				byte b = bytes[i++];
				if ((b & 0x80) == 0) {
					buffer.append((char) (b & 0x7F));
				} else if ((b & 0xE0) != 0xE0) {
					buffer.append((char) (((b & 0x1F) << 6) | (bytes[i++] & 0x3F)));
				} else {
					buffer.append((char) (((b & 0x0F) << 12)
							| ((bytes[i++] & 0x3F) << 6) | (bytes[i++] & 0x3F)));
				}
			}
		}
	}

	public static int writeString(DataOutput out, String s) throws IOException {
		if (s.length() > 0xffff / 3) {
			s = s.substring(0, 0xffff / 3);
		}

		int len = utf8Length(s);
		if (len > 0xffff) // double-check length
			throw new IOException("string too long!");

		out.writeShort(len);
		writeChars(out, s, 0, s.length());
		return len;
	}

	private static int utf8Length(String string) {
		int stringLength = string.length();
		int utf8Length = 0;
		for (int i = 0; i < stringLength; i++) {
			int c = string.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				utf8Length++;
			} else if (c > 0x07FF) {
				utf8Length += 3;
			} else {
				utf8Length += 2;
			}
		}
		return utf8Length;
	}

	private static void writeChars(DataOutput out, String s, int start,
			int length) throws IOException {
		final int end = start + length;
		for (int i = start; i < end; i++) {
			int code = s.charAt(i);
			if (code >= 0x01 && code <= 0x7F) {
				out.writeByte((byte) code);
			} else if (code <= 0x07FF) {
				out.writeByte((byte) (0xC0 | ((code >> 6) & 0x1F)));
				out.writeByte((byte) (0x80 | code & 0x3F));
			} else {
				out.writeByte((byte) (0xE0 | ((code >> 12) & 0X0F)));
				out.writeByte((byte) (0x80 | ((code >> 6) & 0x3F)));
				out.writeByte((byte) (0x80 | (code & 0x3F)));
			}
		}
	}

}
