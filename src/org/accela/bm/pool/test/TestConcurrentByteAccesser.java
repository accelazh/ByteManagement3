package org.accela.bm.pool.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.ByteAccesserFactory;
import org.accela.bm.pool.impl.ConcurrentByteAccesser;
import org.accela.bm.pool.impl.RafByteAccesser;

public class TestConcurrentByteAccesser extends TestByteAccesser
{
	private static class MockByteAccesser implements ByteAccesser
	{
		private Thread hostThread = null;

		@Override
		public synchronized void get(byte[] buf,
				int bufOffset,
				long idx,
				int length) throws IOException
		{
			assert (hostThread == null);
			hostThread = Thread.currentThread();
			try
			{
				Thread.sleep(20);
			}
			catch (InterruptedException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
			hostThread = null;
		}

		@Override
		public synchronized long length() throws IOException
		{
			assert (hostThread == null);
			hostThread = Thread.currentThread();
			try
			{
				Thread.sleep(20);
			}
			catch (InterruptedException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
			hostThread = null;
			return 0;
		}

		@Override
		public synchronized void set(byte[] buf,
				int bufOffset,
				long idx,
				int length) throws IOException
		{
			assert (hostThread == null);
			hostThread = Thread.currentThread();
			try
			{
				Thread.sleep(20);
			}
			catch (InterruptedException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
			hostThread = null;
		}

		@Override
		public synchronized void setLength(long newLength) throws IOException
		{
			assert (hostThread == null);
			hostThread = Thread.currentThread();
			try
			{
				Thread.sleep(20);
			}
			catch (InterruptedException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
			hostThread = null;
		}

		@Override
		public synchronized void flush() throws IOException
		{
			assert (hostThread == null);
			hostThread = Thread.currentThread();
			try
			{
				Thread.sleep(20);
			}
			catch (InterruptedException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
			hostThread = null;
		}

		@Override
		public synchronized void close() throws IOException
		{
			assert (hostThread == null);
			hostThread = Thread.currentThread();
			try
			{
				Thread.sleep(20);
			}
			catch (InterruptedException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
			hostThread = null;
		}

	}

	@Override
	protected void open(Object... args) throws IOException
	{
		if (args[0] == Boolean.FALSE)
		{
			this.accesser = new ConcurrentByteAccesser(16,
					new ByteAccesserFactory()
					{
						@Override
						public ByteAccesser create() throws IOException
						{
							return new MockByteAccesser();
						}
					});
		}
		else
		{
			this.accesser = new ConcurrentByteAccesser(16,
					new ByteAccesserFactory()
					{
						@Override
						public ByteAccesser create() throws IOException
						{
							return new RafByteAccesser(testFile);
						}
					});
		}
	}

	private void readWriteTest(long pos, int length, boolean check)
			throws IOException
	{
		byte[] bs = genBytes(length);
		byte[] buf = new byte[length];
		accesser.set(bs, 0, pos, bs.length);
		accesser.get(buf, 0, pos, buf.length);
		assert (!check || Arrays.equals(bs, buf));
	}

	private void concurrentReadWrite(int num,
			boolean diffId,
			boolean forward,
			boolean check)
	{
		final boolean FORWARD = forward;
		final boolean CHECK = check;
		final int NUM = num;
		final int LEN = 100;

		class Worker implements Runnable
		{
			private int id = 0;

			public Worker(int id)
			{
				this.id = id;
			}

			@Override
			public void run()
			{
				try
				{
					for (int i = 0; i < 10; i++)
					{
						readWriteTest((id + (FORWARD ? i : 0) * NUM) * LEN,
								LEN,
								CHECK);
					}
				}
				catch (IOException ex)
				{
					ex.printStackTrace();
					assert (false);
				}
			}
		}

		Thread[] threads = new Thread[NUM];
		for (int i = 0; i < threads.length; i++)
		{
			threads[i] = new Thread(new Worker(diffId ? i : threads.length / 2));
			threads[i].start();
		}

		for (int i = 0; i < threads.length; i++)
		{
			try
			{
				threads[i].join();
			}
			catch (InterruptedException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
		}
	}

	public void testConcurrentReadWrite() throws FileNotFoundException
	{
		concurrentReadWrite(1, true, true, true);
		concurrentReadWrite(8, true, true, true);
		concurrentReadWrite(15, true, true, true);
		concurrentReadWrite(16, true, true, true);
		concurrentReadWrite(32, true, true, true);
	}

	public void testSharedReadWrite()
	{
		concurrentReadWrite(1, false, false, false);
		concurrentReadWrite(8, false, false, false);
		concurrentReadWrite(15, false, false, false);
		concurrentReadWrite(16, false, false, false);
		concurrentReadWrite(32, false, false, false);
	}

	public void testLock() throws IOException
	{
		reopen(false);
		assert (((ConcurrentByteAccesser) accesser).getFactory().create() instanceof MockByteAccesser);
		concurrentReadWrite(32, false, false, false);
	}

	public void testSetLength() throws IOException
	{
		long len = accesser.length();
		long newLen = len + 1024;
		accesser.setLength(newLen);
		assert (accesser.length() == newLen);
		assert (testFile.length() == newLen);
	}

}
