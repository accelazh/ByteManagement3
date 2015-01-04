package org.accela.bm.pool.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.accela.bm.common.DataFormatException;
import org.accela.bm.common.test.TestBase;
import org.accela.bm.pool.BlockIndexOutOfBoundsException;
import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.BytePool;
import org.accela.bm.pool.test.SectionList.Section;

//manual: 测试用例方法必须一个一个单独地运行，以保证测试输出文件不打架
//manual: 测试前需要把代码中的print语句打开，以输出可能的错误信息
public abstract class TestBytePool extends TestBase
{
	protected ByteAccesser accesser = null;

	protected BytePool pool = null;

	private boolean skipFindCount = false;

	@Override
	protected void tearDown() throws Exception
	{
		super.tearDown();

		System.out.println();
		System.out.println("=================================================");
		System.out.println("==========start to find warning outputs==========");
		System.out.println("=================================================");
		System.out.println();
		if (skipFindCount)
		{
			System.out.println("skipped!");
		}

		int count = skipFindCount ? 0 : new FindFailErrorFound().find();
		assert (0 == count) : count;
	}

	@Override
	protected void close() throws IOException
	{
		if (pool != null)
		{
			pool.close();
		}
	}

	@Override
	protected abstract void open(Object... args) throws IOException;

	public void testIndexError() throws IOException
	{
		skipFindCount = true;

		long key = pool.alloc(100);
		try
		{
			pool.get(key, 128);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof BlockIndexOutOfBoundsException);
		}
		try
		{
			pool.get(key, new byte[128], 10, 50, 79);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof BlockIndexOutOfBoundsException);
		}
		try
		{
			pool.set(key, 128, (byte) 10);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof BlockIndexOutOfBoundsException);
		}
		try
		{
			pool.set(key, new byte[80], 10, 50, 79);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof BlockIndexOutOfBoundsException);
		}
		try
		{
			pool.get(key, new byte[20], 10, 50, 78);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof IndexOutOfBoundsException);
		}

		boolean succ = pool.get(key + 1, new byte[80], 10);
		assert (!succ);

		succ = pool.set(key + 1, new byte[80], 10);
		assert (!succ);

		for (int i = 0; i < 2000; i++)
		{
			if (key == i)
			{
				byte b = (byte) rand.nextInt();
				succ = pool.set(i, 5, b);
				assert (succ);
				assert (pool.get(i, 5) == b);
			}
			else
			{
				succ = pool.get(i, new byte[0], 0);
				assert (!succ);
			}
		}

		succ = pool.free(key);
		assert (succ);
		succ = pool.free(key);
		assert (!succ);

		// 测试分配零大小的空间
		for (int i = 0; i < 100; i++)
		{
			try
			{
				key = pool.alloc(0);
				pool.get(key, new byte[0], 0);
			}
			catch (Exception ex)
			{
				assert (false);
			}
			try
			{
				pool.get(key, new byte[0], 32);
				assert (false);
			}
			catch (Exception ex)
			{
				assert (ex instanceof BlockIndexOutOfBoundsException);
			}
			try
			{
				succ = pool.free(key);
				assert (succ);
			}
			catch (Exception ex)
			{
				assert (false);
			}
		}

		// 测试读取零大小
		key = pool.alloc(100);
		try
		{
			pool.get(key, 125);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof BlockIndexOutOfBoundsException);
		}
		try
		{
			pool.get(key, new byte[0], 125);
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			assert (false);
		}
		try
		{
			pool.get(key, new byte[0], 127);
			assert (false);
		}
		catch (Exception ex)
		{
			assert (ex instanceof BlockIndexOutOfBoundsException);
		}

	}

	// manual: 检查打印的shrink信息
	// manual: 在输出中寻找fail、error、not found字样
	public void testShrink() throws IOException
	{
		assert (!skipFindCount);

		Stack<Long> keys = new Stack<Long>();
		for (int i = 0; i < 100; i++)
		{
			keys.push(pool.alloc(rand.nextInt(1024 * 1024 + 1)));
			keys.push(pool.alloc(rand.nextInt(1024 * 100 + 1)));
			keys.push(pool.alloc(rand.nextInt(1024 * 10 + 1)));
			keys.push(pool.alloc(rand.nextInt(1024 + 1)));
			keys.push(pool.alloc(rand.nextInt(100 + 1)));
			keys.push(pool.alloc(rand.nextInt(64 + 1)));
			keys.push(pool.alloc(rand.nextInt(32 + 1)));
			keys.push(pool.alloc(rand.nextInt(10 + 1)));
			keys.push(pool.alloc(rand.nextInt(1 + 1)));
			keys.push(pool.alloc(rand.nextInt(0 + 1)));
		}

		while (keys.size() > 0)
		{
			boolean succ = pool.free(keys.pop());
			assert (succ);
		}

		assert (accesser.length() == 0);
		assert (keys.isEmpty());

		// ============================================================
		System.out.println("round 2");

		for (int i = 0; i < 1000; i++)
		{
			keys.push(pool.alloc(1000));
		}

		while (keys.size() > 0)
		{
			boolean succ = pool.free(keys.pop());
			assert (succ);
		}

		assert (accesser.length() == 0);
		assert (keys.isEmpty());
	}

	// 高频alloc/free，测试分配区域是否重叠
	// load block header failure 可能是因为一只线程正在写一个Block的header，
	// 而另一个线程试图读之。没事，这是同步机制中考虑到的，是一种代替锁的防止
	// 方式多线程同时访问Block的方式
	// manual: 在输出中寻找fail、error、not found字样，用FindFailErrorFound来做
	// manual: 测试的时候，将MAX_LEN_EXP降低到可测试的程度，测试临界大块
	// manual: MAX_LEN_EXP: 18、19、20 to 30
	public void testFreqAllocFree()
	{
		assert (!skipFindCount);

		final SectionList list = new SectionList();

		final int NUM = 20;

		Thread[] threads = new Thread[NUM];
		for (int i = 0; i < threads.length; i++)
		{
			threads[i] = new Thread(new Runnable()
			{
				@Override
				public void run()
				{
					try
					{
						for (int i = 0; i < 300; i++)
						{
							System.out.println(Thread.currentThread()
									+ "i: "
									+ i);
							System.out.println("SectionList.size(): "
									+ list.size());

							boolean succ = false;
							double rnd = rand.nextDouble();
							if (rnd < 0.1)
							{
								long key = pool.alloc(1024 * 512);
								assert (key >= 0);
								succ = list.add(new Section(key, 1024 * 512));
								assert (succ);
							}
							else if (rnd < 0.35)
							{
								long key = pool.alloc(1024 * 50);
								assert (key >= 0);
								succ = list.add(new Section(key, 1024 * 50));
								assert (succ);
							}
							else if (rnd < 0.6)
							{
								long key = pool.alloc(1024);
								assert (key >= 0);
								succ = list.add(new Section(key, 1024));
								assert (succ);
							}
							else if (rnd < 0.75)
							{
								Section section = null;
								synchronized (list)
								{
									if (list.size() > 0)
									{
										section = list.remove(rand.nextInt(list
												.size()));
									}
								}
								if (section != null)
								{
									long key = pool.realloc(section.getPos(),
											section.getLength() * 2);
									assert (key >= 0);
									succ = list.add(new Section(key, section
											.getLength() * 2));
									assert (succ);
								}
							}
							else if (rnd < 0.85)
							{
								Section section = null;
								synchronized (list)
								{
									if (list.size() > 0)
									{
										section = list.remove(rand.nextInt(list
												.size()));
									}
								}
								if (section != null)
								{
									long key = pool.realloc(section.getPos(),
											section.getLength() * 3);
									assert (key >= 0);
									succ = list.add(new Section(key, section
											.getLength() * 3));
									assert (succ);
								}
							}
							else
							{
								Section section = null;
								synchronized (list)
								{
									if (list.size() > 0)
									{
										section = list.remove(rand.nextInt(list
												.size()));
									}
								}
								if (section != null)
								{
									succ = pool.free(section.getPos());
									assert (succ);
								}
							}

						}// end of for
					}
					catch (IOException ex)
					{
						ex.printStackTrace();
					}
				}
			});
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

	private Map<Long, byte[]> alloc(SectionList list, int count, int length)
			throws IOException
	{
		Map<Long, byte[]> map = new HashMap<Long, byte[]>();

		for (int i = 0; i < count; i++)
		{
			System.out.println("alloc i: " + i);

			byte[] buf = genBytesRnd(length);
			long key = pool.alloc(buf.length);
			if (list != null)
			{
				assert (list.canInsert(new Section(key, buf.length)));
			}
			assert (!map.containsKey((Long) key));
			assert (key >= 0 && key < pool.poolSize());
			boolean succ = pool.set(key,
					buf,
					buf.length / 3,
					buf.length / 3,
					buf.length / 3);
			byte[] buf2 = new byte[buf.length];
			succ = pool.get(key, buf2, 0);
			for (int j = 0; j < buf.length / 3; j++)
			{
				assert (buf2[j + buf.length / 3] == buf[j + buf.length / 3]);
			}
			succ = pool.set(key, buf, 0);
			assert (succ);
			rand.nextBytes(buf2);
			succ = pool.get(key, buf2, 0);
			assert (succ);
			assert (Arrays.equals(buf, buf2));

			key = pool.realloc(key, buf.length);
			if (list != null)
			{
				assert (list.canInsert(new Section(key, buf.length)));
			}
			rand.nextBytes(buf2);
			succ = pool.get(key, buf2, 0);
			assert (succ);
			assert (Arrays.equals(buf, buf2));

			byte[] buf3 = new byte[buf.length / 2];
			key = pool.realloc(key, buf.length / 2);
			if (list != null)
			{
				assert (list.canInsert(new Section(key, buf.length)));
			}
			rand.nextBytes(buf3);
			succ = pool.get(key, buf3, 0);
			assert (succ);
			assert (Arrays.equals(Arrays.copyOf(buf, buf.length / 2), buf3));

			key = pool.realloc(key, buf.length * 2);
			rand.nextBytes(buf3);
			succ = pool.set(key,
					buf3,
					0,
					buf.length * 2 - buf3.length,
					buf3.length);
			assert (succ);
			rand.nextBytes(buf2);
			succ = pool.get(key, buf2, 0);
			assert (succ);
			assert (Arrays.equals(buf, buf2));
			succ = pool.get(key, buf2, buf2.length);
			assert (succ);
			for (int j = 0; j < buf3.length; j++)
			{
				assert (buf3[j] == buf2[buf2.length - buf3.length + j]);
			}

			assert (!map.containsKey((Long) key));
			map.put(key, buf);
			if (list != null)
			{
				succ = list.add(new Section(key, buf.length * 2));
				assert (succ);
			}
		}

		return map;
	}

	private void free(SectionList list, Map<Long, byte[]> map)
			throws IOException
	{
		for (long key : map.keySet())
		{
			byte[] buf = new byte[map.get(key).length];
			boolean succ = pool.get(key, buf, 0);
			assert (succ);
			assert (Arrays.equals(buf, map.get(key)));
			if (list != null)
			{
				succ = list.remove(key);
				assert (succ);
			}
			succ = pool.free(key);
			assert (succ);
		}
	}

	private Map<Long, String> fill(SectionList list, int count, int length)
			throws IOException
	{
		Map<Long, String> map = new HashMap<Long, String>();
		try
		{
			for (int i = 0; i < count; i++)
			{
				System.out.println("fill i: " + i);

				String str = this.genStrRnd(length);
				long key = pool.put(str, delegate);
				if (list != null)
				{
					assert (list.canInsert(new Section(key, delegate
							.writeBytes(str).length)));
				}
				assert (!map.containsKey((Long) key));

				String strOut = pool.fetch(key, delegate);
				assert (strOut.equals(str));

				str = this.genStrRnd(length);
				key = pool.replace(key, str, delegate);
				strOut = pool.fetch(key, delegate);
				assert (strOut.equals(str));

				assert (!map.containsKey((Long) key));
				map.put(key, str);
				if (list != null)
				{
					boolean succ = list.add(new Section(key, delegate
							.writeBytes(str).length));
					assert (succ);
				}

			}
		}
		catch (DataFormatException ex)
		{
			ex.printStackTrace();
			assert (false);
		}

		return map;
	}

	private void remove(SectionList list, Map<Long, String> map)
			throws IOException
	{
		try
		{
			for (long key : map.keySet())
			{
				String str = pool.fetch(key, delegate);
				assert (str.equals(map.get(key)));
				if (list != null)
				{
					boolean succ = list.remove(key);
					assert (succ);
				}
				boolean succ = pool.remove(key);
				assert (succ);
			}
		}
		catch (DataFormatException ex)
		{
			ex.printStackTrace();
			assert (false);
		}
	}

	// manual: 查看pool shrink by half，查看block reuse
	// manual: 测试的时候，将MAX_LEN_EXP降低到可测试的程度，测试临界大块
	// manual: 在输出中寻找fail、error、not found字样
	// load block header failure 可能是因为一只线程正在写一个Block的header，
	// 而另一个线程试图读之。没事，这是同步机制中考虑到的，是一种代替锁的防止
	// 方式多线程同时访问Block的方式
	private void readWriteTest(SectionList list, boolean singleThread)
			throws IOException
	{
		long oldLength = accesser.length();
		System.out.println("oldLength: " + oldLength);

		free(list, alloc(list, 10, 1024 * 512));
		remove(list, fill(list, 20, 1024 * 16));
		if (singleThread)
		{
			assert (accesser.length() == oldLength);
		}

		free(list, alloc(list, 20, 1024 * 100));
		remove(list, fill(list, 20, 1024 * 16));
		if (singleThread)
		{
			assert (accesser.length() == oldLength);
		}

		free(list, alloc(list, 100, 2000));
		remove(list, fill(list, 100, 2000));
		if (singleThread)
		{
			assert (accesser.length() == oldLength);
		}

		free(list, alloc(list, 100, 256));
		remove(list, fill(list, 100, 256));
		if (singleThread)
		{
			assert (accesser.length() == oldLength);
		}

		free(list, alloc(list, 100, 32));
		remove(list, fill(list, 100, 32));
		if (singleThread)
		{
			assert (accesser.length() == oldLength);
		}

		free(list, alloc(list, 100, 0));
		remove(list, fill(list, 100, 0));
		if (singleThread)
		{
			assert (accesser.length() == oldLength);
		}

		Map<Long, byte[]> bmap = new HashMap<Long, byte[]>();
		bmap.putAll(alloc(list, 10, 1024 * 1024));
		bmap.putAll(alloc(list, 100, 1024 * 100));
		bmap.putAll(alloc(list, 100, 1024));
		bmap.putAll(alloc(list, 100, 100));

		Map<Long, String> smap = new HashMap<Long, String>();
		smap.putAll(fill(list, 20, 1024 * 16));
		smap.putAll(fill(list, 100, 1024));
		smap.putAll(fill(list, 100, 100));

		remove(list, smap);
		free(list, bmap);
		if (singleThread)
		{
			assert (accesser.length() == oldLength);
		}

	}

	// MAX_LEN_EXP最小取25能保完成（需要去24、25）。MAX_LEN_EXP还需要取到19、20、21
	// manual: 检查文件大小不会过大，第一阶段4M，第二阶段32M
	public void testSingleThreadReadWrite() throws IOException
	{
		assert (!skipFindCount);

		SectionList list = new SectionList();
		readWriteTest(list, true);
	}

	// MAX_LEN_EXP最小取30能保完成。MAX_LEN_EXP还需要取到19、20、21
	// manual: 检查文件大小不会过大，第一阶段64M，第二阶段512M(可能会有一瞬间到1G)
	public void testConThreadReadWrite() throws IOException
	{
		assert (!skipFindCount);

		final int NUM = 20;
		final SectionList list = new SectionList();

		Thread[] threads = new Thread[NUM];
		for (int i = 0; i < threads.length; i++)
		{
			threads[i] = new Thread(new Runnable()
			{
				@Override
				public void run()
				{
					try
					{
						readWriteTest(list, false);
					}
					catch (IOException ex)
					{
						throw new RuntimeException(ex);
					}
				}
			});
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
		assert (list.size() == 0);
		// assert (accesser.length() == 0); // 多线程可能导致block merge不完全，故无法保证该条件
	}

	public void testRelloc() throws IOException
	{
		assert (!skipFindCount);

		// test zero size relloc
		long key = pool.alloc(1024 * 1024);
		pool.realloc(key, 0);

		// test shrink realloc
		byte[] buf = genBytes(1234);
		key = pool.alloc(1234);
		pool.set(key, buf, 0);
		key = pool.realloc(key, 100);
		byte[] bufOut = genBytes(1234);
		pool.get(key, bufOut, 0, 0, 100);
		assert (Arrays.equals(Arrays.copyOf(buf, 100),
				Arrays.copyOf(bufOut, 100)));

		buf = genBytes(1234 * 1234);
		key = pool.alloc(1234 * 1234);
		pool.set(key, buf, 0);
		key = pool.realloc(key, 100);
		bufOut = genBytes(1234 * 1234);
		pool.get(key, bufOut, 0, 0, 100);
		assert (Arrays.equals(Arrays.copyOf(buf, 100),
				Arrays.copyOf(bufOut, 100)));

		// test expand realloc
		buf = genBytes(1234);
		key = pool.alloc(1234);
		pool.set(key, buf, 0);
		key = pool.realloc(key, 1234 * 1234);
		bufOut = genBytes(1234 * 1234);
		pool.get(key, bufOut, 0, 0, 1234);
		assert (Arrays.equals(Arrays.copyOf(buf, 1234),
				Arrays.copyOf(bufOut, 1234)));

		buf = genBytes(1234 * 1234);
		pool.set(key, buf, 0);
		pool.get(key, bufOut, 0);
		assert (Arrays.equals(buf, bufOut));

		// test massive realloc
		buf = genBytes(1234 * 1234);
		key = pool.alloc(buf.length);
		pool.set(key, buf, 0);
		key = pool.realloc(key, 1234 * 2345 * 7);
		bufOut = genBytes(1234 * 1234);
		pool.get(key, bufOut, 0);
		assert (Arrays.equals(buf, bufOut));

		rand.nextBytes(bufOut);
		boolean succ = pool.set(key, buf, 0, buf.length, buf.length);
		succ = succ && pool.get(key, bufOut, 0, bufOut.length, bufOut.length);
		assert (succ);
		assert (Arrays.equals(buf, bufOut));

		// test massive realloc 2
		buf = genBytes(1024 * 1024);
		key = pool.alloc(buf.length);
		pool.set(key, buf, 0);
		key = pool.realloc(key, 1024 * 1024 * 8);
		bufOut = genBytes(1024 * 1024);
		pool.get(key, bufOut, 0);
		assert (Arrays.equals(buf, bufOut));

		rand.nextBytes(bufOut);
		succ = pool.set(key, buf, 0, buf.length, buf.length);
		succ = succ && pool.get(key, bufOut, 0, bufOut.length, bufOut.length);
		assert (succ);
		assert (Arrays.equals(buf, bufOut));

	}

	public void testPersistance() throws IOException
	{
		testPersistanceImpl(null);
	}

	private void testPersistanceImpl(List<String> additionalSkips)
			throws IOException
	{
		assert (!skipFindCount);

		SectionList list = new SectionList();
		Map<Long, byte[]> map = alloc(list, 100, 5000);
		reopen(true);
		free(list, map);
		assert (list.size() == 0);

		map = alloc(list, 100, 5000);
		reopen(false);
		for (long key : map.keySet())
		{
			boolean succ = pool.free(key);
			assert (!succ);
		}
		list.clear();
		map = alloc(list, 100, 5000);
		free(list, map);

		List<String> skips = new LinkedList<String>();
		if (additionalSkips != null)
		{
			skips.addAll(additionalSkips);
		}
		skips.add("Common.tryGetBlock2 error 2");
		int count = new FindFailErrorFound().find(skips);
		assert (0 == count) : count;

		skipFindCount = true;
	}

	public void testMassiveAlloc() throws IOException
	{
		assert (!skipFindCount);

		final int LEN = 1024 * 1024 * 64;
		long key = pool.alloc(LEN);
		byte[] buf = genBytes(LEN);
		pool.set(key, buf, 0);
		byte[] buf2 = new byte[buf.length];
		pool.get(key, buf2, 0);
		assert (Arrays.equals(buf, buf2));
	}

	private void notContains(Map<Long, byte[]> map, int count)
			throws IOException
	{
		int sum = 0;
		long max = Math.max(100, 2 * pool.poolSize());
		for (int i = 0; i < max; i++)
		{
			if (!map.containsKey((long) i))
			{
				boolean succ = pool.get(i, new byte[0], 0);
				succ = succ || pool.free(i);
				if (succ)
				{
					sum++;
				}
			}
		}
		assert (sum <= count) : sum;
	}

	public void testProtection() throws IOException
	{
		skipFindCount = true;

		Map<Long, byte[]> map = new HashMap<Long, byte[]>();
		notContains(map, 0);

		map = alloc(null, 10, 500);
		notContains(map, 0);

		fillFileRandom(3000);
		notContains(map, 0);

		RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
		byte[] buf = new byte[(int) pool.poolSize()];
		Arrays.fill(buf, (byte) 0);
		raf.write(buf);
		notContains(new HashMap<Long, byte[]>(), 0);
	}

	private Map<Long, String> splitMap(Map<Long, String> map, double rate)
	{
		Map<Long, String> toRemove = new HashMap<Long, String>();
		int count = (int) (map.keySet().size() * rate);
		for (int i = 0; i < count; i++)
		{
			Long key = map.keySet().toArray(new Long[0])[rand.nextInt(map
					.keySet().size())];
			String ret = map.remove(key);
			assert (ret != null);
			toRemove.put(key, ret);
		}

		return toRemove;
	}

	// manual: 查看Block reuse 情况
	// 查看打印的merge、shrink、divide信息
	public void testReuseBlock() throws IOException
	{
		assert (!skipFindCount);

		SectionList list = new SectionList();
		Map<Long, String> map = new HashMap<Long, String>();
		map.putAll(fill(list, 1000, 1024));
		int oldMapSize = map.size();
		long fileLen = accesser.length();
		System.out.println("file length: " + fileLen);

		for (int i = 0; i < 10; i++)
		{
			remove(list, splitMap(map, 0.333));
			if (i % 2 == 1)
			{
				reopen(true);
			}
			map.putAll(fill(list, 333, 1024));
			assert (accesser.length() <= fileLen);
			assert (map.size() >= oldMapSize * 0.9);
		}
	}

	public void testErrorFetch() throws IOException
	{
		assert (!skipFindCount);

		Map<Long, byte[]> map = alloc(null, 100, 5000);
		for (long key : map.keySet())
		{
			byte[] ret = pool.fetch(key);
			assert (null == ret);
		}
	}

	public void testRandomFileA() throws IOException
	{
		assert (!skipFindCount);

		SectionList list = new SectionList();
		fillFileRandom(1024 * 1024 * 1024);
		reopen(true);
		readWriteTest(list, true);
		testRelloc();
		testPersistance();
		skipFindCount = false;
		testMassiveAlloc();

		System.out.println(accesser.length());

		List<String> skips = new LinkedList<String>();
		skips.add("Common.tryGetBlock2 error 2");
		int count = new FindFailErrorFound().find(skips);
		assert (0 == count) : count;

		skipFindCount = true;
	}

	public void testRandomFileB() throws IOException
	{
		assert (!skipFindCount);

		SectionList list = new SectionList();
		Map<Long, byte[]> map = new HashMap<Long, byte[]>();
		map.putAll(alloc(list, 100, 1024 * 1024));
		map.putAll(alloc(list, 100, 1024 * 100));
		map.putAll(alloc(list, 100, 1024 * 10));
		map.putAll(alloc(list, 100, 1024));
		map.putAll(alloc(list, 100, 512));
		map.putAll(alloc(list, 100, 200));
		map.putAll(alloc(list, 100, 100));
		map.putAll(alloc(list, 100, 50));
		map.putAll(alloc(list, 100, 10));
		map.putAll(alloc(list, 100, 1));
		fillFileRandom(accesser.length());

		readWriteTest(list, true);
		testRelloc();
		List<String> additionalSkips = new LinkedList<String>();
		additionalSkips.add("Block loadHeader failure: check sum invalid");
		additionalSkips.add("Common.tryGetBlock error 4");
		testPersistanceImpl(additionalSkips);
		skipFindCount = false;
		testMassiveAlloc();

		for (long key : map.keySet())
		{
			pool.free(key);
		}

		System.out.println(accesser.length());

		List<String> skips = new LinkedList<String>();
		skips.add("Block loadHeader failure: check sum invalid");
		skips.add("Common.tryGetBlock error 4");
		skips.add("Common.tryGetBlock2 error 2");
		int count = new FindFailErrorFound().find(skips);
		assert (0 == count) : count;

		skipFindCount = true;
	}

	public void testRandomFileC() throws IOException
	{
		assert (!skipFindCount);

		SectionList list = new SectionList();
		Map<Long, byte[]> map = new HashMap<Long, byte[]>();
		map.putAll(alloc(list, 100, 1024 * 1024));
		map.putAll(alloc(list, 100, 1024 * 100));
		map.putAll(alloc(list, 100, 1024 * 10));
		map.putAll(alloc(list, 100, 1024));
		map.putAll(alloc(list, 100, 512));
		map.putAll(alloc(list, 100, 200));
		map.putAll(alloc(list, 100, 100));
		map.putAll(alloc(list, 100, 50));
		map.putAll(alloc(list, 100, 10));
		map.putAll(alloc(list, 100, 1));
		fillFileRandom(accesser.length());

		testConThreadReadWrite();

		for (long key : map.keySet())
		{
			pool.free(key);
		}

		System.out.println(accesser.length());

		List<String> skips = new LinkedList<String>();
		skips.add("Block loadHeader failure: check sum invalid");
		skips.add("Common.tryGetBlock error 4");
		skips.add("Common.tryGetBlock2 error 4");
		int count = new FindFailErrorFound().find(skips);
		assert (0 == count) : count;

		skipFindCount = true;
	}

	private Map<Long, byte[]> allocP(int count, int length) throws IOException
	{
		Map<Long, byte[]> map = new HashMap<Long, byte[]>();

		for (int i = 0; i < count; i++)
		{
			byte[] buf = genBytesRnd(length);
			long key = pool.alloc(buf.length);
			map.put(key, buf);
		}

		return map;
	}

	private void freeP(Map<Long, byte[]> map) throws IOException
	{
		for (long key : map.keySet())
		{
			pool.free(key);
		}
	}

	public void testPerformance() throws IOException
	{
		freeP(allocP(10, 1024 * 1024));
		freeP(allocP(10, 1024 * 100));
		freeP(allocP(10, 1024 * 10));
		freeP(allocP(10, 1024));
		freeP(allocP(10, 512));
		freeP(allocP(10, 200));
		freeP(allocP(10, 100));
		freeP(allocP(10, 50));
		freeP(allocP(10, 10));
		freeP(allocP(10, 1));

		freeP(allocP(10, 1));
		freeP(allocP(10, 10));
		freeP(allocP(10, 50));
		freeP(allocP(10, 100));
		freeP(allocP(10, 200));
		freeP(allocP(10, 512));
		freeP(allocP(10, 1024));
		freeP(allocP(10, 1024 * 10));
		freeP(allocP(10, 1024 * 100));
		freeP(allocP(10, 1024 * 1024));
	}

	public void testPerformanceCon() throws IOException
	{
		final int NUM = 20;

		Thread[] threads = new Thread[NUM];
		for (int i = 0; i < threads.length; i++)
		{
			threads[i] = new Thread(new Runnable()
			{
				@Override
				public void run()
				{
					try
					{
						freeP(allocP(10, 1024 * 1024));
						freeP(allocP(10, 1024 * 100));
						freeP(allocP(10, 1024 * 10));
						freeP(allocP(10, 1024));
						freeP(allocP(10, 512));
						freeP(allocP(10, 200));
						freeP(allocP(10, 100));
						freeP(allocP(10, 50));
						freeP(allocP(10, 10));
						freeP(allocP(10, 1));

						freeP(allocP(10, 1));
						freeP(allocP(10, 10));
						freeP(allocP(10, 50));
						freeP(allocP(10, 100));
						freeP(allocP(10, 200));
						freeP(allocP(10, 512));
						freeP(allocP(10, 1024));
						freeP(allocP(10, 1024 * 10));
						freeP(allocP(10, 1024 * 100));
						freeP(allocP(10, 1024 * 1024));
					}
					catch (IOException ex)
					{
						throw new RuntimeException(ex);
					}
				}
			});
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
}
