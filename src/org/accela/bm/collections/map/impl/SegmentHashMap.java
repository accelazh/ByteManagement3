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

//TODO brief ���˼�룺�������Ұ�resize�ͷ�resize״̬��Ϊһ�µ�
//TODO �Ժ�Ѷ��߳�ģ�ͺúü��һ��
//TODO ����ģ�ͻ��ǲ�����࣬������һЩ����û�н��
/*Ŀǰ��tip��
 1. ���СΪ2���ݴΣ���ô���Ƶ�ʱ��Ԫ��Ҫô����ԭλ��Ҫô���Ƶ�ԭλ+���С/2�ĵط���
 �����������ź���Է�Ϊ�ϰ벿�ֺ��°벿�֣��������������㶨Ϊ����С��ָ�룬��ʾ
 ��ǰ���ڰ��Ƶĵ�Ԫ��
 2. putԪ�ط���ʱ������ָ��λ�ã�����������ͬ���ԣ��Ƿ����°벿���Ƿ����ϰ汾����Ȼλ��
 ��ʱ�ȴ��������ơ�
 3. ֻ��ά���°벿�İ���ָ�뼴�ɣ�����Լ��ָ��һ�¾�δ���ƣ�ָ�����Ͼ����ƹ����������ڰ�
 �Ʋ��Ѿ�������
 4. Ϊ��ͳһ״̬������ͳһresizing״̬������״̬������һ��״̬��
 */

//TODO put�����������ҵ�index��ͻȻ����expand���������Դ������Կ�����rwlock����Ҳ���Կ����� �������㷨����������ô��
//TODO 2010-9-15 ���ڵĲ�������̫�����ˣ��Ժ�Ҫ��һ�����ŵķ������У����ڶ��ϣ��������Ȳ���
//TODO put/get��access������format�¹�ϣ�����顢����Ԫ�ء����Ź�ϣ����Щ�����Ļ������ʵ�������Դ���
//TODO put/get�Ȳ��������ܵ�Ԫ�ذ���ʱ�����ƹ�����Ԫ���Ҳ�����Ӱ�죬Ҳ���ѽ������ŵ�ͬ������
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

	// ����Źֵ����֣��μ�resize�㷨
	// Move����ʱ�ʾ������ϣ�����ź󣬰Ѿɱ��е�Ԫ�ذ��˵��±�Ĳ���
	// DownHalf, UpHalfָ��ϣ�����ź󣬱�Ĵ�С���ԭ�������������¼�����ԭ����С���±��
	// ԭ�����ǲ��־ͽ�upHalf���¼��ϵ��ǲ��־ͽ�downHalf
	private SyncLong downHalfMoveIdx = null;

	// �����Ѿ���ɰ��ƹ����ٵ�Ԫ������ͬ������
	private AtomicLong moveCount = null;

	private AtomicLong size = null;

	private Lock expandLock = new ReentrantLock();

	// Note�� ���Կ��ǻ���ReaWriteResourceLocker
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

		// assert (BytePool.NULL == -1); //TODO ����������BytePool.NULL==-1
		this.table.fill((byte) 0xff);
		byte[] buf = new byte[Common.LONG_SIZE];
		this.table.getElement(this.table.size() - 1, buf, 0, 0, buf.length);
		assert (Common.byteToLong(buf, 0) == -1);

		this.downHalfMoveIdx = new SyncLong(this.table.size());
		this.moveCount = new AtomicLong(this.table.size());
		this.size = new AtomicLong(0);

		assert (!isExpanding());

		// TODO write������Ҫ�Ѹ���ָ�룬moveIdx��moveCount������
		// TODO moveCount��MoveIdx��read��ʱ���������tableSize���Զ�����
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
		// ���������Ϊ�˱�֤��table��������Ĺ����У�������������
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

	// TODO: ��java.util.HashMap�Ĺ�ϣ��������֪���µĶԲ��ԣ���Ҫ���ԡ�
	// �����ϣ�㷨��Ŀ�����ڻ�ϸ�λ�͵�λ�ı��أ���Դ�СΪ2���ݴεı�
	// ���͹�ϣֵ���λ��ͬ�������䵽���ͬһ���еĸ���
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
		// TODO ��ʼ��downHalfIdx��Ȼ���������
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

	// TODO addǰ��node�ĳ�next����ĳ�Ա����Ӧ���趨��
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
