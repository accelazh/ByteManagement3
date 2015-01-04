package org.accela.bm.common;

public class Common
{
	public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;

	public static final int INTEGER_SIZE = Integer.SIZE / Byte.SIZE;

	public static final byte MAX_EXP_FOR_LONG = (byte) (Long.SIZE - 2);

	public static final long MAX_LONG_FROM_EXP = expToValue(MAX_EXP_FOR_LONG);

	public static final byte MAX_EXP_FOR_INT = (byte) (Integer.SIZE - 2);

	public static final int MAX_INT_FROM_EXP = (int) expToValue(MAX_EXP_FOR_INT);

	public static void longToByte(byte[] buf, int off, long value)
	{
		buf[off + 0] = (byte) (value >>> 56);
		buf[off + 1] = (byte) (value >>> 48);
		buf[off + 2] = (byte) (value >>> 40);
		buf[off + 3] = (byte) (value >>> 32);
		buf[off + 4] = (byte) (value >>> 24);
		buf[off + 5] = (byte) (value >>> 16);
		buf[off + 6] = (byte) (value >>> 8);
		buf[off + 7] = (byte) (value >>> 0);
	}

	public static long byteToLong(byte[] buf, int off)
	{
		return (((long) buf[off + 0] << 56)
				+ ((long) (buf[off + 1] & 255) << 48)
				+ ((long) (buf[off + 2] & 255) << 40)
				+ ((long) (buf[off + 3] & 255) << 32)
				+ ((long) (buf[off + 4] & 255) << 24)
				+ ((buf[off + 5] & 255) << 16)
				+ ((buf[off + 6] & 255) << 8) + ((buf[off + 7] & 255) << 0));
	}

	public static void intToByte(byte[] buf, int off, int value)
	{
		buf[0 + off] = (byte) ((value >>> 24) & 0xFF);
		buf[1 + off] = (byte) ((value >>> 16) & 0xFF);
		buf[2 + off] = (byte) ((value >>> 8) & 0xFF);
		buf[3 + off] = (byte) ((value >>> 0) & 0xFF);
	}

	public static int byteToInt(byte[] buf, int off)
	{
		int ch1 = buf[0 + off] & 0xFF;
		int ch2 = buf[1 + off] & 0xFF;
		int ch3 = buf[2 + off] & 0xFF;
		int ch4 = buf[3 + off] & 0xFF;
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	public static String bitStr(long value)
	{
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < Long.SIZE; i++)
		{
			buf.append((value & 0x01) + "");
			value >>= 1;
		}
		buf.reverse();
		return buf.toString();
	}

	public static long expToValue(byte exp)
	{
		if (exp < 0 || exp > MAX_EXP_FOR_LONG)
		{
			throw new IllegalArgumentException("exp illegal: " + exp);
		}

		return (((long) 0x01) << exp);
	}

	public static byte valueToExp(long value)
	{
		if (value < 0 || value > MAX_LONG_FROM_EXP)
		{
			throw new IllegalArgumentException("value illegal: " + value);
		}

		byte count = 0;
		long trying = 0x01;
		while (value > trying)
		{
			trying <<= 1;
			count++;
		}

		assert (count >= 0);
		return count;
	}

	public static long valueToExpValue(long value)
	{
		return expToValue(valueToExp(value));
	}

	public static byte checkSum(byte value)
	{
		return (byte) (value ^ (-1));
	}

	public static byte[] checkSum(byte[] data)
	{
		if (null == data)
		{
			throw new IllegalArgumentException("data should not be null");
		}

		byte[] checkSum = new byte[data.length];
		for (int i = 0; i < data.length; i++)
		{
			checkSum[i] = checkSum(data[i]);
		}
		return checkSum;
	}

	public static void main(String[] args)
	{
		System.out.println(valueToExp(9));
		System.out.println(MAX_EXP_FOR_LONG);
		System.out.println(bitStr(MAX_LONG_FROM_EXP));

		System.out.println(MAX_EXP_FOR_INT);
		System.out.println(MAX_INT_FROM_EXP);
		System.out.println(bitStr(MAX_INT_FROM_EXP));
	}
}
