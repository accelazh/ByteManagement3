package org.accela.bm.pool.impl;

import java.io.IOException;

import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.ByteAccesserFactory;

//Note：子ByteAccesser必须自己管理同步，多个线程可能别派发到同一个子ByteAccesser上
public class ConcurrentByteAccesser implements ByteAccesser
{
	private int concurrencyLevel = 1;

	private ByteAccesserFactory factory = null;

	private ByteAccesser[] accessers = null;

	private IndexDispatcher dispatcher = new IndexDispatcher();

	private class IndexDispatcher
	{
		private int idx = 0;

		public synchronized int getIdx()
		{
			int ret = idx;
			idx++;
			if (idx >= concurrencyLevel)
			{
				idx = 0;
			}

			return ret;
		}
	}

	public ConcurrentByteAccesser(int concurrencyLevel,
			ByteAccesserFactory factory) throws IOException
	{
		if (concurrencyLevel < 1)
		{
			throw new IllegalArgumentException("concurrencyLevel illegal: "
					+ concurrencyLevel);
		}
		if (null == factory)
		{
			throw new IllegalArgumentException("factory should not be null");
		}

		this.concurrencyLevel = concurrencyLevel;
		this.factory = factory;

		initAccessers();
	}

	private void initAccessers() throws IOException
	{
		this.accessers = new ByteAccesser[concurrencyLevel];
		for (int i = 0; i < accessers.length; i++)
		{
			accessers[i] = factory.create();
		}
	}

	public int getConcurrencyLevel()
	{
		return concurrencyLevel;
	}

	public ByteAccesserFactory getFactory()
	{
		return factory;
	}

	private ByteAccesser selectAccesser()
	{
		return accessers[dispatcher.getIdx()];
	}

	@Override
	public void flush() throws IOException
	{
		for (int i = 0; i < accessers.length; i++)
		{
			accessers[i].flush();
		}
	}

	@Override
	public void close() throws IOException
	{
		for (int i = 0; i < accessers.length; i++)
		{
			accessers[i].close();
		}
	}

	@Override
	public void get(byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		selectAccesser().get(buf, bufOffset, idx, length);
	}

	@Override
	public void set(byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		selectAccesser().set(buf, bufOffset, idx, length);
	}

	@Override
	public long length() throws IOException
	{
		return selectAccesser().length();
	}

	@Override
	public void setLength(long newLength) throws IOException
	{
		for (int i = 0; i < accessers.length; i++)
		{
			if (accessers[i].length() != newLength)
			{
				accessers[i].setLength(newLength);
			}
		}
	}

}
