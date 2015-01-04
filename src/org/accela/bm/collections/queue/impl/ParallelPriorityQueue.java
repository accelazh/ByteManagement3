package org.accela.bm.collections.queue.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.accela.bm.collections.queue.BytePriorityQueue;
import org.accela.bm.collections.queue.ByteQueue;
import org.accela.bm.common.Concurrentable;
import org.accela.bm.common.DataFormatException;

public class ParallelPriorityQueue implements BytePriorityQueue, Concurrentable
{
	public static interface ChildQueueFactory
	{
		// 新建即可，不需要create/read方法初始化
		public ByteQueue create(int priority, long elementLength)
				throws IOException;
	}

	private static class SyncChildQueueFactory implements ChildQueueFactory
	{
		private ChildQueueFactory factory = null;

		public SyncChildQueueFactory(ChildQueueFactory factory)
		{
			if (null == factory)
			{
				throw new IllegalArgumentException("null factory");
			}
			this.factory = factory;
		}

		public ChildQueueFactory getFactory()
		{
			return factory;
		}

		@Override
		public ByteQueue create(int priority, long elementLength)
				throws IOException
		{
			ByteQueue queue = factory.create(priority, elementLength);
			if (!(queue instanceof Concurrentable))
			{
				queue = new SynchronizedByteQueue(queue);
			}

			assert (queue instanceof Concurrentable);
			return queue;
		}

	}

	private SyncChildQueueFactory factory = null;

	// 优先级->子队列的映射，优先级可以是不连续的非负整数
	// 需要用rwlock保护
	private Map<Integer, ByteQueue> map = null;

	// 用于记录优先级的顺序性，便于顺序查询
	// 需要用rwlock保护
	private NavigableSet<Integer> set = null;

	private ReadWriteLock rwlock = new ReentrantReadWriteLock();

	private long elementLength = 0;

	public ParallelPriorityQueue(ChildQueueFactory factory, long elementLength)
	{
		if (null == factory)
		{
			throw new IllegalArgumentException("null factory");
		}
		if (elementLength < 1)
		{
			throw new IllegalArgumentException("illegal elementLength: "
					+ elementLength);
		}

		this.factory = new SyncChildQueueFactory(factory);
		this.elementLength = elementLength;
	}

	public ChildQueueFactory getFactory()
	{
		return factory.getFactory();
	}

	@Override
	public long getElementLength()
	{
		return elementLength;
	}

	@Override
	public void create(Object... args) throws IOException
	{
		map = new HashMap<Integer, ByteQueue>();
		set = new TreeSet<Integer>();
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		map = new HashMap<Integer, ByteQueue>();
		set = new TreeSet<Integer>();

		int num = in.readInt();
		rwlock.writeLock().lock();
		try
		{
			for (int i = 0; i < num; i++)
			{
				int priority = in.readInt();
				if (priority < 0)
				{
					continue;
				}

				ByteQueue queue = factory.create(priority, elementLength);
				queue.read(in);
				map.put(priority, queue);
				set.add(priority);
			}
		}
		finally
		{
			rwlock.writeLock().unlock();
		}

		assert (map.size() == set.size());
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		rwlock.readLock().lock();
		try
		{
			assert (map.size() == set.size());
			out.writeInt(map.size());
			for (Map.Entry<Integer, ByteQueue> entry : map.entrySet())
			{
				assert (entry.getKey() >= 0);
				assert (entry.getValue() != null);

				out.writeInt(entry.getKey());
				entry.getValue().write(out);
			}
		}
		finally
		{
			rwlock.readLock().unlock();
		}
	}

	@Override
	public long size()
	{
		long sum = 0;
		rwlock.readLock().lock();
		try
		{
			for (ByteQueue queue : map.values())
			{
				sum += queue.size();
			}
		}
		finally
		{
			rwlock.readLock().unlock();
		}

		return sum;
	}

	// TODO 检查getQueue方法，新建queue和查找queue
	// TODO 检查建立了大而不连续的priority的队列，是否影响速度
	@Override
	public ByteQueue getQueue(int priority) throws IOException
	{
		if (priority < 0)
		{
			throw new IllegalArgumentException("illegal priority: " + priority);
		}

		ByteQueue ret = null;
		rwlock.readLock().lock();
		try
		{
			ret = map.get(priority);
		}
		finally
		{
			rwlock.readLock().unlock();
		}
		if (ret != null)
		{
			return ret;
		}

		rwlock.writeLock().lock();
		try
		{
			ret = map.get(priority);
			if (ret != null)
			{
				return ret;
			}

			ret = factory.create(priority, this.elementLength);
			assert (ret instanceof Concurrentable);
			ret.create();

			map.put(priority, ret);
			assert (!set.contains(priority));
			set.add(priority);
			assert (map.size() == set.size());
			return ret;
		}
		finally
		{
			rwlock.writeLock().unlock();
		}

	}

	@Override
	public void clear() throws IOException
	{
		rwlock.readLock().lock();
		try
		{
			for (ByteQueue queue : map.values())
			{
				queue.clear();
			}
		}
		finally
		{
			rwlock.readLock().unlock();
		}
	}

	@Override
	public void enqueue(int priority,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		this.getQueue(priority).addTail(buf, bufOffset, idxInElement, length);
	}

	@Override
	public boolean dequeueBy(int priority,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		return this.getQueue(priority).removeHead(buf,
				bufOffset,
				idxInElement,
				length);
	}

	@Override
	public int dequeueHigh(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		rwlock.readLock().lock();
		try
		{
			Iterator<Integer> ascItr = set.iterator();
			while (ascItr.hasNext())
			{
				int priority = ascItr.next();
				assert (priority >= 0);
				boolean succ = map.get(priority).removeHead(buf,
						bufOffset,
						idxInElement,
						length);
				if (succ)
				{
					return priority;
				}
			}

			return -1;
		}
		finally
		{
			rwlock.readLock().unlock();
		}
	}

	// TODO 检查没有子队列的时候的工作情况
	@Override
	public int dequeueLow(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		rwlock.readLock().lock();
		try
		{
			Iterator<Integer> descItr = set.descendingIterator();
			while (descItr.hasNext())
			{
				int priority = descItr.next();
				assert (priority >= 0);
				boolean succ = map.get(priority).removeHead(buf,
						bufOffset,
						idxInElement,
						length);
				if (succ)
				{
					return priority;
				}
			}

			return -1;
		}
		finally
		{
			rwlock.readLock().unlock();
		}
	}

	@Override
	public boolean peekBy(int priority,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		return getQueue(priority).getHead(buf, bufOffset, idxInElement, length);
	}

	@Override
	public int peekHigh(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException
	{
		rwlock.readLock().lock();
		try
		{
			Iterator<Integer> ascItr = set.iterator();
			while (ascItr.hasNext())
			{
				int priority = ascItr.next();
				assert (priority >= 0);
				boolean succ = map.get(priority).getHead(buf,
						bufOffset,
						idxInElement,
						length);
				if (succ)
				{
					return priority;
				}
			}

			return -1;
		}
		finally
		{
			rwlock.readLock().unlock();
		}
	}

	@Override
	public int peekLow(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException
	{
		rwlock.readLock().lock();
		try
		{
			Iterator<Integer> descItr = set.descendingIterator();
			while (descItr.hasNext())
			{
				int priority = descItr.next();
				assert (priority >= 0);
				boolean succ = map.get(priority).getHead(buf,
						bufOffset,
						idxInElement,
						length);
				if (succ)
				{
					return priority;
				}
			}

			return -1;
		}
		finally
		{
			rwlock.readLock().unlock();
		}
	}

}
