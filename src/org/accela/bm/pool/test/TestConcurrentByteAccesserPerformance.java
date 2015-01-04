package org.accela.bm.pool.test;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.accela.bm.common.test.TestBase;
import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.ByteAccesserFactory;
import org.accela.bm.pool.impl.ConcurrentByteAccesser;
import org.accela.bm.pool.impl.RafByteAccesser;

public class TestConcurrentByteAccesserPerformance extends TestBase
{
	private ByteAccesserFactory factory = null;

	private ConcurrentByteAccesser cb = null;

	private RafByteAccesser rb = null;

	private byte[] data = null;

	private byte[] buf = null;

	@Override
	protected void close() throws IOException
	{
		if (cb != null)
		{
			cb.close();
		}
		if (rb != null)
		{
			rb.close();
		}
	}

	@Override
	protected void open(Object... args) throws IOException
	{
		factory = new ByteAccesserFactory()
		{
			@Override
			public ByteAccesser create() throws IOException
			{
				return new RafByteAccesser(testFile);
			}
		};
		cb = new ConcurrentByteAccesser(4, factory);
		rb = (RafByteAccesser) factory.create();

		data = genBytes(1024 * 1024);
		buf = new byte[data.length];

		RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
		raf.setLength(data.length);
	}

	private int[] genRange(int mode)
	{
		if (0 == mode)
		{
			int a = rand.nextInt(data.length);
			int b = rand.nextInt(data.length);
			return new int[] { Math.min(a, b), Math.abs(a - b) };
		}
		else if (1 == mode)
		{
			int ret[] = new int[2];
			ret[0] = rand.nextInt(data.length);
			ret[1] = /* rand.nextInt( */Math.min(data.length - ret[0], 32)/* ) */;
			return ret;
		}
		else
		{
			assert (false);
			return null;
		}

	}

	private void readWrite(ByteAccesser accesser, int mode) throws IOException
	{
		int[] range = genRange(mode);
		accesser.set(data, range[0], range[0], range[1]);
		range = genRange(mode);
		accesser.set(buf, range[0], range[0], range[1]);
	}

	private long readWriteRun(final ByteAccesser accesser,
			final int num,
			final int mode)
	{
		class Worker implements Runnable
		{
			@Override
			public void run()
			{
				try
				{
					for (int i = 0; i < 20; i++)
					{
						readWrite(accesser, mode);
					}
				}
				catch (IOException ex)
				{
					ex.printStackTrace();
					assert (false);
				}
			}
		}

		Thread[] threads = new Thread[num];
		for (int i = 0; i < threads.length; i++)
		{
			threads[i] = new Thread(new Worker());
		}

		long startTime = System.nanoTime();
		for (int i = 0; i < threads.length; i++)
		{
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
		long endTime = System.nanoTime();
		return endTime - startTime;
	}

	// 测试结果表明，mode=0千字节大块读写，ConcurrentByteAccesser效率很好，在2倍左右
	// mode=1一百字节一下的小块读写，ConcurrentByteAccesser效率提升不明显，
	// 与RandomByteAccesser基本一致
	// 本机concurrencyLevel==4时，已经不能通过提高concurrencyLevel获得更高的性能
	public void testPerformance()
	{
		final int num = 16;
		final int mode = 0;
		System.out.println("round 1");
		System.out.println("cb: " + readWriteRun(cb, num, mode));
		System.out.println("rb: " + readWriteRun(rb, num, mode));
		System.out.println("round 2");
		System.out.println("cb: " + readWriteRun(cb, num, mode));
		System.out.println("rb: " + readWriteRun(rb, num, mode));
		System.out.println("round 3");
		System.out.println("cb: " + readWriteRun(cb, num, mode));
		System.out.println("rb: " + readWriteRun(rb, num, mode));
	}

}
