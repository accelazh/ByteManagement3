package org.accela.util.test;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.accela.util.ResourceLocker;

public class TestResourceLockerPerformance
{
	private static final int NUM_THREAD = 200;

	private static final int RESOURCE_SIZE = 1024 * 1024;

	private static final int WORK_REPETITION = 1000;

	private static final int WORK_SIZE = 100;

	private static String[] resource = null;

	private static Random random = new Random();

	private static Lock lock = new ReentrantLock();

	private static Object lockObj = new Object();

	private static ResourceLocker<Integer> rlock = new ResourceLocker<Integer>();

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		for (int count = 0; count < 3; count++)
		{
			System.out.println("count: " + count);
			test();
			System.out.println();
		}
	}

	private static void test()
	{
		resource = new String[RESOURCE_SIZE];
		for (int i = 0; i < resource.length; i++)
		{
			resource[i] = "";
		}

		Thread[] noLockThreads = new Thread[NUM_THREAD];
		Thread[] lockWholeThreads = new Thread[NUM_THREAD];
		Thread[] rlockThreads = new Thread[NUM_THREAD];
		Thread[] syncThreads = new Thread[NUM_THREAD];
		for (int i = 0; i < NUM_THREAD; i++)
		{
			noLockThreads[i] = new Thread(new NoLockWorker());
			lockWholeThreads[i] = new Thread(new LockWholeWorker());
			rlockThreads[i] = new Thread(new ResourceLockerWorker());
			syncThreads[i] = new Thread(new SyncWorker());
		}

		Thread[][] threads = new Thread[][] {
				rlockThreads,
				lockWholeThreads,
				syncThreads,
				noLockThreads };

		for (int i = 0; i < threads.length; i++)
		{
			// long startTime = System.nanoTime();
			for (int j = 0; j < threads[i].length; j++)
			{
				threads[i][j].start();
			}
			for (int j = 0; j < threads[i].length; j++)
			{
				try
				{
					threads[i][j].join();
				}
				catch (InterruptedException ex)
				{
					ex.printStackTrace();
				}
			}
			// long endTime = System.nanoTime();

			// System.out.println((endTime - startTime)
			// + "\t"
			// + showMapTableLen((HashMap<?, ?>) rlock.lockMap));
		}
	}

	// private static String showMapTableLen(HashMap<?, ?> map)
	// {
	// Field field = null;
	// try
	// {
	// field = map.getClass().getDeclaredField("table");
	//
	// field.setAccessible(true);
	// return "" + ((Object[]) field.get(map)).length;
	// }
	// catch (SecurityException ex)
	// {
	// ex.printStackTrace();
	// }
	// catch (NoSuchFieldException ex)
	// {
	// ex.printStackTrace();
	// }
	// catch (IllegalArgumentException ex)
	// {
	// ex.printStackTrace();
	// }
	// catch (IllegalAccessException ex)
	// {
	// ex.printStackTrace();
	// }
	//
	// return "error";
	// }

	private abstract static class AbstractWorker implements Runnable
	{
		@Override
		public void run()
		{
			for (int i = 0; i < WORK_REPETITION; i++)
			{
				int idx = findIdx();
				lock(idx);
				try
				{
					work(idx);
				}
				finally
				{
					unlock(idx);
				}
			}
		}

		private int findIdx()
		{
			return random.nextInt(resource.length);
		}

		protected void work(int idx)
		{
			// long startTime = System.nanoTime();
			for (int i = 0; i < WORK_SIZE; i++)
			{
				resource[idx] = "";
				resource[idx] += random.nextLong() + "";
			}
			// long endTime = System.nanoTime();
			// System.out.println("duration: " + (endTime - startTime));

			// try
			// {
			// Thread.sleep(1);
			// }
			// catch (InterruptedException ex)
			// {
			// ex.printStackTrace();
			// }
		}

		protected abstract void lock(int idx);

		protected abstract void unlock(int idx);
	}

	private static class NoLockWorker extends AbstractWorker
	{
		@Override
		protected void lock(int idx)
		{
			// do nothing
		}

		@Override
		protected void unlock(int idx)
		{
			// do nothing
		}
	}

	private static class LockWholeWorker extends AbstractWorker
	{
		@Override
		protected void lock(int idx)
		{
			lock.lock();
		}

		@Override
		protected void unlock(int idx)
		{
			lock.unlock();
		}
	}

	private static class ResourceLockerWorker extends AbstractWorker
	{
		@Override
		protected void lock(int idx)
		{
			rlock.lock(idx);
		}

		@Override
		protected void unlock(int idx)
		{
			rlock.unlock(idx);
		}
	}

	private static class SyncWorker extends AbstractWorker
	{
		@Override
		protected void work(int idx)
		{
			synchronized (lockObj)
			{
				super.work(idx);
			}
		}

		@Override
		protected void lock(int idx)
		{
			// do nothing
		}

		@Override
		protected void unlock(int idx)
		{
			// do nothing
		}
	}

}
