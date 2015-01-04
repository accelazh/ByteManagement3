package org.accela.bm.pool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.accela.bm.common.DataFormatException;
import org.accela.bm.common.PersistanceDelegate;

public class TolerantBytePool implements BytePool
{
	private BytePool pool = null;

	public TolerantBytePool(BytePool pool)
	{
		if (null == pool)
		{
			throw new IllegalArgumentException("pool is null");
		}

		this.pool = pool;
	}

	public BytePool getPool()
	{
		return pool;
	}

	private void onFailure(BlockIndexOutOfBoundsException ex)
	{
		System.err
				.println("TolerantBytePool catches a BlockIndexOutOfBoundsException!");
		ex.printStackTrace();
	}

	@Override
	public long poolSize()
	{
		return pool.poolSize();
	}

	@Override
	public long alloc(long size) throws IOException
	{
		return pool.alloc(size);
	}

	@Override
	public void close() throws IOException
	{
		pool.close();
	}

	@Override
	public void create(Object... args) throws IOException
	{
		pool.create(args);
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		pool.read(in);
	}

	@Override
	public long realloc(long key, long size) throws IOException
	{
		while (true)
		{
			try
			{
				return pool.realloc(key, size);
			}
			catch (BlockIndexOutOfBoundsException ex)
			{
				onFailure(ex);
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		pool.write(out);
	}

	@Override
	public boolean set(long key, long idx, byte value) throws IOException
	{
		try
		{
			return pool.set(key, idx, value);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return false;
		}
	}

	@Override
	public boolean set(long key, byte[] buf, long idx) throws IOException
	{
		try
		{
			return pool.set(key, buf, idx);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return false;
		}
	}

	@Override
	public boolean set(long key, byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		try
		{
			return pool.set(key, buf, bufOffset, idx, length);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return false;
		}
	}

	@Override
	public long put(byte[] data) throws IOException
	{
		while (true)
		{
			try
			{
				return pool.put(data);
			}
			catch (BlockIndexOutOfBoundsException ex)
			{
				onFailure(ex);
			}
		}
	}

	@Override
	public <T> long put(T object, PersistanceDelegate<T> delegate)
			throws IOException
	{
		while (true)
		{
			try
			{
				return pool.put(object, delegate);
			}
			catch (BlockIndexOutOfBoundsException ex)
			{
				onFailure(ex);
			}
		}
	}

	@Override
	public byte[] fetch(long key) throws IOException
	{
		try
		{
			return pool.fetch(key);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return new byte[0];
		}
	}

	@Override
	public <T> T fetch(long key, PersistanceDelegate<T> delegate)
			throws IOException, DataFormatException
	{
		try
		{
			return pool.fetch(key, delegate);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			throw new DataFormatException(ex);
		}
	}

	@Override
	public void flush() throws IOException
	{
		pool.flush();
	}

	@Override
	public ByteAccesser getAccesser()
	{
		return pool.getAccesser();
	}

	@Override
	public boolean free(long key) throws IOException
	{
		return pool.free(key);
	}

	@Override
	public byte get(long key, long idx) throws IOException
	{
		try
		{
			return pool.get(key, idx);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return 0;
		}
	}

	@Override
	public boolean get(long key, byte[] buf, long idx) throws IOException
	{
		try
		{
			return pool.get(key, buf, idx);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return false;
		}
	}

	@Override
	public boolean get(long key, byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		try
		{
			return pool.get(key, buf, bufOffset, idx, length);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return false;
		}
	}

	@Override
	public long replace(long key, byte[] data) throws IOException
	{
		while (true)
		{
			try
			{
				return pool.replace(key, data);
			}
			catch (BlockIndexOutOfBoundsException ex)
			{
				onFailure(ex);
			}
		}
	}

	@Override
	public <T> long replace(long key, T object, PersistanceDelegate<T> delegate)
			throws IOException
	{
		while (true)
		{
			try
			{
				return pool.replace(key, object, delegate);
			}
			catch (BlockIndexOutOfBoundsException ex)
			{
				onFailure(ex);
			}
		}
	}

	@Override
	public boolean remove(long key) throws IOException
	{
		try
		{
			return pool.remove(key);
		}
		catch (BlockIndexOutOfBoundsException ex)
		{
			onFailure(ex);
			return false;
		}
	}

}
