package org.accela.bm.collections.map.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.accela.bm.collections.array.SegmentArray;
import org.accela.bm.collections.common.ElementEqualityComparator;
import org.accela.bm.collections.common.ElementHasher;
import org.accela.bm.common.Common;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.pool.BytePool;
import org.accela.bm.pool.TolerantBytePool;
import org.accela.util.ResourceLocker;

//TODO brief 设计思想：别忘了我把resize和非resize状态视为一致的
//TODO 以后把多线程模型好好检查一遍
//TODO 并发模型还是不够简洁，并且有一些问题没有解决
/*目前的tip：
 1. 表大小为2的幂次，那么搬移的时候，元素要么留在原位，要么被移到原位+表大小/2的地方。
 这样，表扩张后可以分为上半部分和下半部分，用两个相隔距离恒定为半表大小的指针，表示
 当前正在搬移的单元格。
 2. put元素放入时，根据指针位置，可以做出不同策略，是放入下半部还是放入上版本，虽然位置
 过时等待后续搬移。
 3. 只用维护下半部的搬移指针即可，可以约定指针一下均未搬移，指针以上均搬移过，或者正在搬
 移并已经加锁。
 4. 为了统一状态，可以统一resizing状态和正常状态，当作一个状态。
 */

//TODO put操作，就怕找到index后，突然发生expand操作，难以处理；可以考虑用rwlock做，也可以考虑用 非阻塞算法，但都不怎么样
//TODO 2010-9-15 现在的并发处理太复杂了，以后要找一个优雅的方法才行，现在多哈希表的理解深度不够
//TODO put/get等access方法、format新哈希表数组、搬移元素、扩张哈希表，这些操作的互相干扰实在是难以处理
//TODO put/get等操作，会受到元素搬移时，搬移过程中元素找不到的影响，也很难进行优雅的同步控制
public class SegmentHashMap extends AbstractByteMap
{
	private TolerantBytePool pool = null;

	private ElementEqualityComparator comparator = null;

	private ElementHasher hasher = null;

	private long segmentSize = 0;

	private int keyLength = 0;

	private long valueLength = 0;

	private SegmentArray table = null;

	private float loadFactor = 0.75f;

	// 这个古怪的名字，参见resize算法
	// Move这个词表示，当哈希表扩张后，把旧表中的元素搬运到新表的操作
	// DownHalf, UpHalf指哈希表扩张后，表的大小变成原来的两倍，即新加上了原来大小的新表格。
	// 原来的那部分就叫upHalf，新加上的那部分就叫downHalf
	private SyncLong downHalfMoveIdx = null;

	// 描述已经完成搬移过多少单元格，用于同步控制
	private AtomicLong moveCount = null;

	private AtomicLong size = null;

	private Lock expandLock = new ReentrantLock();

	// Note： 可以考虑换成ReaWriteResourceLocker
	private ResourceLocker<Long> locker = new ResourceLocker<Long>();

	public SegmentHashMap(BytePool pool,
			ElementEqualityComparator comparator,
			ElementHasher hasher,
			long segmentSize,
			int keyLength,
			long valueLength,
			float loadFactor)
	{
		if (null == pool)
		{
			throw new NullPointerException("null pool");
		}
		if (null == comparator)
		{
			throw new NullPointerException("null comparator");
		}
		if (null == hasher)
		{
			throw new NullPointerException("null hasher");
		}
		if (segmentSize < 2)
		{
			throw new IllegalArgumentException("illegal segmentSize: "
					+ segmentSize);
		}
		if (keyLength < 1)
		{
			throw new IllegalArgumentException("illegal keyLength: "
					+ keyLength);
		}
		if (valueLength < 1)
		{
			throw new IllegalArgumentException("illegal valueLength: "
					+ valueLength);
		}
		if (loadFactor < 0)
		{
			throw new IllegalArgumentException("loadFactor < 0: " + loadFactor);
		}

		this.pool = new TolerantBytePool(pool);
		this.comparator = comparator;
		this.hasher = hasher;
		this.segmentSize = Common.expToValue(Common.valueToExp(segmentSize));
		this.keyLength = keyLength;
		this.valueLength = valueLength;
		this.loadFactor = loadFactor;
	}

	public BytePool getPool()
	{
		return this.pool.getPool();
	}

	public ElementEqualityComparator getComparator()
	{
		return this.comparator;
	}

	public ElementHasher getHasher()
	{
		return this.hasher;
	}

	public long getSegmentSize()
	{
		return this.segmentSize;
	}

	@Override
	public int getKeyLength()
	{
		return this.keyLength;
	}

	@Override
	public long getValueLength()
	{
		return this.valueLength;
	}

	public float getLoadFactor()
	{
		return this.loadFactor;
	}

	@Override
	public long size()
	{
		return this.size.get();
	}

	@Override
	public void create(Object... args) throws IOException
	{
		int initSegNum = (args != null && args[0] instanceof Integer) ? (Integer) args[0]
				: 2;
		initSegNum = Math.max(2, initSegNum);
		long longSegNum = Common.expToValue(Common.valueToExp(initSegNum));
		initSegNum = (int) Math.min(longSegNum,
				Common.expToValue((byte) (Integer.SIZE - 2)));
		this.table = new SegmentArray(pool.getPool(), Common.LONG_SIZE,
				this.segmentSize);
		this.table.create(initSegNum);
		assert (this.table.size() >= 4);

		// assert (BytePool.NULL == -1); //TODO 这里依赖于BytePool.NULL==-1
		this.table.fill((byte) 0xff);
		byte[] buf = new byte[Common.LONG_SIZE];
		this.table.getElement(this.table.size() - 1, buf, 0, 0, buf.length);
		assert (Common.byteToLong(buf, 0) == -1);

		this.downHalfMoveIdx = new SyncLong(this.table.size());
		this.moveCount = new AtomicLong(this.table.size());
		this.size = new AtomicLong(0);

		assert (!isExpanding());

		// TODO write操作需要把各种指针，moveIdx、moveCount记下来
		// TODO moveCount、MoveIdx在read的时候，如果大于tableSize，自动调整
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		// TODO
	}

	@Override
	public void write(DataOutput in) throws IOException
	{
		// TODO
	}

	private void formatTableAt(long idx) throws IOException
	{
		byte[] buf = new byte[Common.LONG_SIZE];
		Common.longToByte(buf, 0, BytePool.NULL);
		table.setElement(idx, buf, 0, 0, buf.length);
	}

	private long tableSize()
	{
		long ret = table.size();
		assert (Common.expToValue(Common.valueToExp(ret)) == ret);
		return ret;
	}

	private long halfTableSize()
	{
		long ret = tableSize() / 2;
		assert (Common.expToValue(Common.valueToExp(ret)) == ret);
		return ret;
	}

	private long getMoveCount()
	{
		assert (this.moveCount.get() <= tableSize());
		assert (this.moveCount.get() <= this.downHalfMoveIdx.get());
		assert (this.moveCount.get() >= halfTableSize());
		return this.moveCount.get();
	}

	private void incrMoveCount()
	{
		this.moveCount.incrementAndGet();
		assert (this.moveCount.get() <= this.downHalfMoveIdx.get());
	}

	private boolean shouldMove()
	{
		assert (this.downHalfMoveIdx.get() <= tableSize());
		assert (this.moveCount.get() <= this.downHalfMoveIdx.get());
		return this.downHalfMoveIdx.get() < tableSize();
	}

	private boolean isMoving()
	{
		return getMoveCount() < tableSize();
	}

	private boolean isExpanding()
	{
		return shouldMove() || isMoving();
	}

	private boolean shouldExpand()
	{
		return !isExpanding()
				&& Common.valueToExp(tableSize()) < Common.MAX_EXP_FOR_LONG
				&& size() > tableSize() * loadFactor;
	}

	private long downHalfMoveIdx()
	{
		assert (this.downHalfMoveIdx.get() <= tableSize());
		assert (this.moveCount.get() <= this.downHalfMoveIdx.get());
		assert (this.downHalfMoveIdx.get() >= halfTableSize());
		return this.downHalfMoveIdx.get();
	}

	private void incrMoveIdx()
	{
		this.downHalfMoveIdx.incr();
	}

	private long calUpHalfMoveIdx(long downHalfMoveIdx)
	{
		return downHalfMoveIdx - halfTableSize();
	}

	private long upHalfMoveIdx()
	{
		return calUpHalfMoveIdx(this.downHalfMoveIdx());
	}

	private void startExpanding() throws IOException
	{
		assert (!isExpanding());
		// 这个上锁是为了保证，table正在扩大的过程中，封锁其它访问
		synchronized (table)
		{
			int oldSegNum = table.getSegmentNum();
			assert (Common.expToValue(Common.valueToExp(oldSegNum)) == oldSegNum);
			for (int i = 0; i < oldSegNum; i++)
			{
				table.addSegment();
			}
		}
	}

	private boolean tryStartExpanding() throws IOException
	{
		if (shouldExpand())
		{
			expandLock.lock();
			try
			{
				if (shouldExpand())
				{
					startExpanding();
					return true;
				}
			}
			finally
			{
				expandLock.unlock();
			}
		}

		return false;
	}

	private void incrSize() throws IOException
	{
		this.size.incrementAndGet();
		tryStartExpanding();
	}

	private void decrSize()
	{
		this.size.decrementAndGet();
	}

	private void lockTableAt(long idx)
	{
		assert (idx >= 0 && idx < tableSize());
		locker.lock(idx);
	}

	private void unlockTableAt(long idx)
	{
		assert (idx >= 0 && idx < tableSize());
		locker.unlock(idx);
	}

	// TODO: 仿java.util.HashMap的哈希函数，不知道仿的对不对，需要测试。
	// 这个哈希算法的目的在于混合高位和低位的比特，针对大小为2的幂次的表，
	// 降低哈希值因低位相同而被分配到表的同一格中的概率
	private long hash(long h)
	{
		h ^= (h >>> 49) ^ (h >>> 32);
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	private int leastMovePerStep()
	{
		return Math.max(1, (int) (1.0 / this.loadFactor));
	}

	private void moveByStep()
	{
		int leastMovePerStep = leastMovePerStep();
		for (int i = 0; i < leastMovePerStep; i++)
		{
			moveByOne();
		}
	}

	private long acquireIdxToMove()
	{
		if (!shouldMove())
		{
			return -1;
		}

		synchronized (this.downHalfMoveIdx)
		{
			if (!shouldMove())
			{
				return -1;
			}

			long downHalfIdx = this.downHalfMoveIdx();

			this.lockTableAt(downHalfIdx);
			this.lockTableAt(this.upHalfMoveIdx());

			this.incrMoveIdx();

			return downHalfIdx;
		}
	}

	private void releaseIdxToMove(long downHalfIdx)
	{
		this.unlockTableAt(calUpHalfMoveIdx(downHalfIdx));
		this.unlockTableAt(downHalfIdx);
	}

	private void moveByOne()
	{
		long downHalfIdx = acquireIdxToMove();
		if (downHalfIdx < 0)
		{
			return;
		}

		moveData(downHalfIdx);

		releaseIdxToMove(downHalfIdx);
		this.incrMoveCount();
	}

	private void moveData(final long downHalfIdx)
	{
		// TODO 初始化downHalfIdx格，然后搬移数据
		final long upHalfIdx = calUpHalfMoveIdx(downHalfIdx);
	}

	private long indexForTableSize(long hash)
	{
		return hash & (tableSize() - 1);
	}

	private long indexForHalfTableSize(long hash)
	{
		return hash & (halfTableSize() - 1);
	}

	private long keyToHash(byte[] keyBuf, int keyBufOffset)
	{
		// TODO
	}

	// TODO add前，node的除next以外的成员变量应该设定过
	private void addNode(long idx, Node node)
	{
		// TODO
	}

	private static class NodePosition
	{
		public long idx = -1;
		public Node prev = null;
		public Node cur = null;

		public NodePosition(long idx, Node prev, Node cur)
		{
			this.idx = idx;
			this.prev = prev;
			this.cur = cur;
		}

	}

	private NodePosition findNode(byte[] keyBuf, int keyBufOffset)
	{

	}

	private void removeNode(NodePosition node)
	{
		// TODO
	}

	private static class SyncLong
	{
		private long value = 0;

		public SyncLong(long value)
		{
			this.value = value;
		}

		public synchronized long get()
		{
			return value;
		}

		public synchronized void set(long value)
		{
			this.value = value;
		}

		public synchronized void incr()
		{
			value++;
		}
	}

	private class Node
	{
		private long key = BytePool.NULL;

		public Node(long key)
		{
			this.key = key;
		}

		public long getKey()
		{
			return key;
		}

		public boolean getNext(long[] next) throws IOException
		{
			byte[] buf = new byte[Common.LONG_SIZE];
			boolean succ = pool.get(key, buf, 0);
			if (succ)
			{
				next[0] = Common.byteToLong(buf, 0);
			}
			return succ;
		}

		public boolean setNext(long next) throws IOException
		{
			byte[] buf = new byte[Common.LONG_SIZE];
			Common.longToByte(buf, 0, next);
			return pool.set(key, buf, 0);
		}

		public boolean getHash(long[] hash) throws IOException
		{
			byte[] buf = new byte[Common.LONG_SIZE];
			boolean succ = pool.get(key, buf, Common.LONG_SIZE);
			if (succ)
			{
				hash[0] = Common.byteToLong(buf, 0);
			}
			return succ;
		}

		public boolean getKey(byte[] buf,
				int bufOffset,
				long idxInElement,
				int length) throws IOException
		{
			if (idxInElement < 0
					|| length < 0
					|| length + idxInElement > getKeyLength())
			{
				throw new IndexOutOfBoundsException(
						"idxInElement or length illegal"
								+ idxInElement
								+ ", "
								+ length);
			}

			return pool.get(key, buf, bufOffset, 2
					* Common.LONG_SIZE
					+ idxInElement, length);
		}

		public boolean getValue(byte[] buf,
				int bufOffset,
				long idxInElement,
				int length) throws IOException
		{
			if (idxInElement < 0
					|| length < 0
					|| length + idxInElement > getKeyLength())
			{
				throw new IndexOutOfBoundsException(
						"idxInElement or length illegal"
								+ idxInElement
								+ ", "
								+ length);
			}

			return pool.get(key, buf, bufOffset, 2
					* Common.LONG_SIZE
					+ getKeyLength()
					+ idxInElement, length);
		}

		public boolean setValue(byte[] buf,
				int bufOffset,
				long idxInElement,
				int length) throws IOException
		{
			if (idxInElement < 0
					|| length < 0
					|| length + idxInElement > getKeyLength())
			{
				throw new IndexOutOfBoundsException(
						"idxInElement or length illegal"
								+ idxInElement
								+ ", "
								+ length);
			}

			return pool.set(key, buf, bufOffset, 2
					* Common.LONG_SIZE
					+ getKeyLength()
					+ idxInElement, length);
		}

		public boolean exist() throws IOException
		{
			return this.getValue(new byte[1], 0, getValueLength() - 1, 1);
		}
	}
}
