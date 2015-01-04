package org.accela.bm.pool.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.accela.bm.pool.ByteAccesser;

public class RafByteAccesser implements ByteAccesser
{
	private File file = null;

	private RandomAccessFile raf = null;

	public RafByteAccesser(File file) throws FileNotFoundException
	{
		if (null == file)
		{
			throw new IllegalArgumentException("file should not be null");
		}

		this.file = file;
		this.raf = new RandomAccessFile(file, "rw");
	}

	public File getFile()
	{
		return this.file;
	}

	@Override
	public synchronized long length() throws IOException
	{
		return raf.length();
	}

	@Override
	public synchronized void setLength(long newLength) throws IOException
	{
		raf.setLength(newLength);
	}

	private void testBound(byte[] buf, int bufOffset, long idx, int length)
	{
		if (null == buf)
		{
			throw new IllegalArgumentException("null buf");
		}
		if (bufOffset < 0)
		{
			throw new IllegalArgumentException("bufOffset < 0");
		}
		if (length < 0)
		{
			throw new IllegalArgumentException("length < 0");
		}
		if (bufOffset + length > buf.length)
		{
			throw new IllegalArgumentException(
					"bufOffset + length > buf.length");
		}
		if (idx < 0)
		{
			throw new IllegalArgumentException("idx < 0");
		}

		return;
	}

	@Override
	public synchronized void get(byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		testBound(buf, bufOffset, idx, length);
		raf.seek(idx);
		raf.readFully(buf, bufOffset, length);
	}

	@Override
	public synchronized void set(byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		testBound(buf, bufOffset, idx, length);
		raf.seek(idx);
		raf.write(buf, bufOffset, length);
	}

	@Override
	public synchronized void flush() throws IOException
	{
		// do nothing
	}

	@Override
	public synchronized void close() throws IOException
	{
		raf.close();
	}

}
