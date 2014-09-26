package org.track.io;

import java.util.Comparator;
import java.util.HashMap;

public class WritableComparator implements Comparator {

	private static HashMap comparators = new HashMap();
	private DataInputBuffer buffer = new DataInputBuffer();
	private Class keyClass;
	private WritableComparable key1;
	private WritableComparable key2;

	public WritableComparator(Class keyClass) {
		this.keyClass = keyClass;
		this.key1 = newKey();
		this.key2 = newKey();
	}

	public WritableComparable newKey() {
		try {
			return (WritableComparable) keyClass.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static synchronized WritableComparator get(Class c) {
		WritableComparator comparator = (WritableComparator) comparators.get(c);
		if (comparator == null)
			comparator = new WritableComparator(c);
		return comparator;
	}

	public static synchronized void define(Class c,
			WritableComparator comparator) {
		comparators.put(c, comparator);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		return a.compareTo(b);
	}

	@Override
	public int compare(Object a, Object b) {
		return compare((WritableComparable) a, (WritableComparable) b);
	}

	public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2,
			int s2, int l2) {
		int end1 = s1 + l1;
		int end2 = s2 + l2;
		for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
			int a = (b1[i] & 0xff);
			int b = (b2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return l1 - l2;
	}

	public static int hashBytes(byte[] bytes, int length) {
		int hash = 1;
		for (int i = 0; i < length; i++)
			hash = (31 * hash) + (int) bytes[i];
		return hash;
	}

	public static int readUnsignedShort(byte[] bytes, int start) {
		return (((bytes[start] & 0xff) << 8) + ((bytes[start + 1] & 0xff)));
	}

	public static int readInt(byte[] bytes, int start) {
		return (((bytes[start] & 0xff) << 24)
				+ ((bytes[start + 1] & 0xff) << 16)
				+ ((bytes[start + 2] & 0xff) << 8) + ((bytes[start + 3] & 0xff)));

	}

	public static float readFloat(byte[] bytes, int start) {
		return Float.intBitsToFloat(readInt(bytes, start));
	}

	public static long readLong(byte[] bytes, int start) {
		return ((long) (readInt(bytes, start)) << 32)
				+ (readInt(bytes, start + 4) & 0xFFFFFFFFL);
	}

}
