package org.accela.bm.pool.impl.binaryPool;

import java.io.IOException;

import org.accela.bm.pool.ByteAccesser;

public class Common
{
	public static final int LONG_SIZE = org.accela.bm.common.Common.LONG_SIZE;

	public static final int INTEGER_SIZE = org.accela.bm.common.Common.INTEGER_SIZE;

	public static final byte MAX_EXP_FOR_LONG = org.accela.bm.common.Common.MAX_EXP_FOR_LONG;

	public static final long MAX_LONG_FROM_EXP = org.accela.bm.common.Common.MAX_LONG_FROM_EXP;

	public static final byte MAX_LEN_EXP = (byte) (MAX_EXP_FOR_LONG - 1);

	public static final byte MIN_LEN_EXP = (byte) Math.ceil((Math
			.log(Block.APPEND_LEN + Block.POINTER_LEN) / Math.log(2)));

	static
	{
		assert (MIN_LEN_EXP >= 1);
		assert (MAX_LEN_EXP > MIN_LEN_EXP);
		assert (MAX_LEN_EXP < MAX_EXP_FOR_LONG);
	}

	public static void longToByte(byte[] buf, int off, long value)
	{
		org.accela.bm.common.Common.longToByte(buf, off, value);
	}

	public static long byteToLong(byte[] buf, int off)
	{
		return org.accela.bm.common.Common.byteToLong(buf, off);
	}

	public static void intToByte(byte[] buf, int off, int value)
	{
		org.accela.bm.common.Common.intToByte(buf, off, value);
	}

	public static int byteToInt(byte[] buf, int off)
	{
		return org.accela.bm.common.Common.byteToInt(buf, off);
	}

	public static String bitStr(long value)
	{
		return org.accela.bm.common.Common.bitStr(value);
	}

	public static boolean checkLenExp(byte lenExp)
	{
		boolean ret = lenExp <= MAX_LEN_EXP && lenExp >= MIN_LEN_EXP;
		if (!ret)
		{
			// System.out.println("Common checkLenExp failure!");
		}
		return ret;
	}

	public static long expToValue(byte exp)
	{
		return org.accela.bm.common.Common.expToValue(exp);
	}

	public static byte valueToExp(long value)
	{
		return org.accela.bm.common.Common.valueToExp(value);
	}

	public static boolean checkAddrMatchLen(long addr, byte lenExp)
	{
		if (addr < 0)
		{
			throw new IllegalArgumentException("addr < 0");
		}
		if (!checkLenExp(lenExp))
		{
			throw new IllegalArgumentException("lenExp illegal");
		}

		boolean ret = (addr & (expToValue(lenExp) - 1)) == 0;
		if (!ret)
		{
			// System.out.println("Common checkAddrMatchLen failure");
		}
		return ret;
	}

	public static Block tryGetBlock(long pos,
			byte lenExp,
			PoolSize poolSize,
			ByteAccesser accesser) throws IOException
	{
		if (null == poolSize)
		{
			throw new IllegalArgumentException("poolSize should not be null");
		}
		if (!Common.checkLenExp(lenExp))
		{
			// System.out.println("Common.tryGetBlock error 1");
			return null;
		}
		if (pos < 0 || pos + expToValue(lenExp) > poolSize.getValue())
		{
			// System.out
			// .print(pos != BytePool.NULL ? "Common.tryGetBlock error 2\n"
			// : "");
			return null;
		}
		if (!Common.checkAddrMatchLen(pos, lenExp))
		{
			// System.out.println("Common.tryGetBlock error 3");
			return null;
		}

		Block block = new Block(pos, accesser);
		boolean succ = block.loadHeader();
		if (!succ)
		{
			// System.out.println("Common.tryGetBlock error 4");
			return null;
		}
		if (block.getLenExp() != lenExp)
		{
			return null;
		}

		return block;
	}

	public static Block tryGetBlock(long pos,
			PoolSize poolSize,
			ByteAccesser accesser) throws IOException
	{
		if (null == poolSize)
		{
			throw new IllegalArgumentException("poolSize should not be null");
		}
		if (pos < 0 || pos + expToValue(MIN_LEN_EXP) > poolSize.getValue())
		{
			// System.out
			// .print(pos != BytePool.NULL ? "Common.tryGetBlock2 error 2\n"
			// : "");
			return null;
		}
		if (!Common.checkAddrMatchLen(pos, MIN_LEN_EXP))
		{
			// System.out.println("Common.tryGetBlock2 error 3");
			return null;
		}

		Block block = new Block(pos, accesser);
		boolean succ = block.loadHeader();
		if (!succ)
		{
			// System.out.println("Common.tryGetBlock2 error 4");
			return null;
		}

		return block;
	}

	public static void main(String[] args)
	{
		byte[] bs = new byte[25];
		intToByte(bs, 1, 155);
		intToByte(bs, 17, Integer.MAX_VALUE / 2);

		System.out.println(byteToInt(bs, 1) == 155);
		System.out.println(byteToInt(bs, 17) == Integer.MAX_VALUE / 2);

		System.out.println(0x01 << (-5));

		System.out.println(bitStr(MAX_LONG_FROM_EXP));
		System.out.println(valueToExp(MAX_LONG_FROM_EXP));

		System.out.println(bitStr((byte) 0xAA));
		System.out.println(bitStr(0x55));

		System.out.println(MIN_LEN_EXP);
		System.out.println(expToValue(MIN_LEN_EXP));
	}

}
