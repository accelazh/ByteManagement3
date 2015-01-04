package org.accela.bm.pool.impl;

import java.io.IOException;
import java.util.Arrays;

import org.accela.bm.common.BytePersistanceDelegate;
import org.accela.bm.common.Common;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.common.PersistanceDelegate;
import org.accela.bm.pool.BytePool;

public abstract class AbstractBytePool implements BytePool
{
	private static final int DATA_HEADER_SIZE = 2 * Common.INTEGER_SIZE;

	// 利用alloc，如果失败，则只有文件空间不足一种情况，抛出异常，
	// 而不是返回一个无效的key
	protected abstract long realloc(long key, long size, boolean copy)
			throws IOException;

	@Override
	public long realloc(long key, long size) throws IOException
	{
		return realloc(key, size, true);
	}

	@Override
	public byte get(long key, long idx) throws IOException
	{
		byte[] buf = new byte[1];
		boolean succ = this.get(key, buf, idx);
		return succ ? buf[0] : 0;
	}

	@Override
	public boolean set(long key, long idx, byte value) throws IOException
	{
		byte[] buf = new byte[] { value };
		return this.set(key, buf, idx);
	}

	@Override
	public boolean get(long key, byte[] buf, long idx) throws IOException
	{
		return this.get(key, buf, 0, idx, buf.length);
	}

	@Override
	public boolean set(long key, byte[] buf, long idx) throws IOException
	{
		return this.set(key, buf, 0, idx, buf.length);
	}

	// =================================================================

	private boolean setArray(long key, byte[] data) throws IOException
	{
		assert (data != null);

		byte[] dataLen = new byte[Common.INTEGER_SIZE];
		Common.intToByte(dataLen, 0, data.length);
		byte[] checkSum = org.accela.bm.common.Common.checkSum(dataLen);

		boolean succ = true;
		succ = succ && this.set(key, dataLen, 0);
		succ = succ && this.set(key, checkSum, dataLen.length);
		succ = succ && this.set(key, data, DATA_HEADER_SIZE);
		return succ;
	}

	@Override
	public long put(byte[] data) throws IOException
	{
		if (null == data)
		{
			throw new IllegalArgumentException("data should not be null");
		}

		long key = alloc(DATA_HEADER_SIZE + data.length);
		setArray(key, data);
		return key;
	}

	@Override
	public <T> long put(T object, PersistanceDelegate<T> delegate)
			throws IOException
	{
		if (null == object)
		{
			throw new IllegalArgumentException("object should not be null");
		}
		if (null == delegate)
		{
			throw new IllegalArgumentException("delegate should not be null");
		}

		return this.put(new BytePersistanceDelegate<T>(delegate)
				.writeBytes(object));
	}

	private byte[] getArray(long key) throws IOException
	{
		byte[] dataLen = new byte[Common.INTEGER_SIZE];
		byte[] checkSum = new byte[dataLen.length];

		boolean succ = true;
		succ = succ && this.get(key, dataLen, 0);
		succ = succ && this.get(key, checkSum, dataLen.length);
		succ = succ
				&& Arrays.equals(org.accela.bm.common.Common.checkSum(dataLen),
						checkSum);

		int dataLenInt = succ ? Common.byteToInt(dataLen, 0) : 0;
		succ = succ && (dataLenInt >= 0);
		byte[] data = succ ? new byte[dataLenInt] : null;
		succ = succ && this.get(key, data, DATA_HEADER_SIZE);

		return succ ? data : null;
	}

	@Override
	public byte[] fetch(long key) throws IOException
	{
		return getArray(key);
	}

	@Override
	public <T> T fetch(long key, PersistanceDelegate<T> delegate)
			throws IOException, DataFormatException
	{
		if (null == delegate)
		{
			throw new IllegalArgumentException("delegate should not be null");
		}

		byte[] data = this.fetch(key);
		if (null == data)
		{
			return null;
		}
		else
		{
			return new BytePersistanceDelegate<T>(delegate).readBytes(data);
		}
	}

	public long replace(long key, byte[] data) throws IOException
	{
		if (null == data)
		{
			throw new IllegalArgumentException("data should not be null");
		}

		key = realloc(key, DATA_HEADER_SIZE + data.length, false);
		setArray(key, data);
		return key;
	}

	public <T> long replace(long key, T object, PersistanceDelegate<T> delegate)
			throws IOException
	{
		if (null == object)
		{
			throw new IllegalArgumentException("object should not be null");
		}
		if (null == delegate)
		{
			throw new IllegalArgumentException("delegate should not be null");
		}

		return this.replace(key,
				new BytePersistanceDelegate<T>(delegate).writeBytes(object));
	}

	@Override
	public boolean remove(long key) throws IOException
	{
		return free(key);
	}
}
