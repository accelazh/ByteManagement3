package org.accela.bm.pool.test;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import org.accela.bm.common.test.TestBase;
import org.accela.bm.pool.ByteAccesser;

public abstract class TestByteAccesser extends TestBase
{
	protected ByteAccesser accesser = null;

	@Override
	protected void close() throws IOException
	{
		if (accesser != null)
		{
			accesser.close();
		}
	}

	@Override
	protected abstract void open(Object... args) throws IOException;

	public void testIndexError() throws IOException
	{
		byte[] bs = new byte[100];
		rand.nextBytes(bs);

		// ======================================

		try
		{
			accesser.get(bs, 0, -1, 10);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.get(bs, 0, 0, -1);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.get(bs, -1, 10, 10);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.get(bs, 0, 0, 101);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.get(bs, 20, 100, 81);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}

		// ==============================

		try
		{
			accesser.get(bs, 0, 0, 10);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof EOFException);
		}

		// ================================

		try
		{
			accesser.set(bs, 0, -1, 10);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.set(bs, 0, 0, -1);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.set(bs, -1, 10, 10);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.set(bs, 0, 0, 101);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}
		try
		{
			accesser.set(bs, 20, 100, 81);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IllegalArgumentException);
		}

		// ==============================

		try
		{
			accesser.set(bs, 0, 0, 10);
			accesser.set(bs, 0, 0, 100);
			accesser.set(bs, 10, 100, 60);
			accesser.set(bs, 20, 101, 80);
		}
		catch (Exception ex)
		{
			assert (false);
		}
	}

	public void testReadWrite() throws IOException
	{
		readWriteTest(0, 100);
		assert (accesser.length() >= 100);
		readWriteTest(50, 100);
		assert (accesser.length() >= 150);
		readWriteTest(1000, 1000);
		assert (accesser.length() >= 2000);
		accesser.flush();

		accesser.setLength(10);
		assert (accesser.length() == 10);

		accesser.close();
	}

	private void readWriteTest(long pos, int length) throws IOException
	{
		byte[] bs = genBytes(length);
		byte[] buf = new byte[length];
		accesser.set(bs, 0, pos, bs.length);
		accesser.get(buf, 0, pos, buf.length);
		assert (Arrays.equals(bs, buf));

		bs = genBytes(length);
		buf = new byte[length];
		accesser.set(bs, bs.length / 3, pos, bs.length / 3);
		accesser.get(buf, buf.length / 3 * 2, pos, buf.length / 3);
		for (int i = 0; i < bs.length / 3; i++)
		{
			assert (bs[bs.length / 3 + i] == buf[buf.length / 3 * 2 + i]);
		}
	}
}
