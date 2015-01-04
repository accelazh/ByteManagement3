package org.accela.bm.collections.map.impl.hashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.accela.bm.collections.array.SegmentArray;
import org.accela.bm.collections.common.ElementEqualityComparator;
import org.accela.bm.collections.common.ElementHasher;
import org.accela.bm.collections.common.ElementViewer;
import org.accela.bm.collections.map.MapContainer;
import org.accela.bm.common.Common;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.common.Persistancer;
import org.accela.bm.pool.BytePool;
import org.accela.bm.pool.TolerantBytePool;
import org.accela.util.ResourceLocker;

public class RawTable implements Persistancer, MapContainer
{
	private TolerantBytePool pool = null;

	private ElementEqualityComparator comparator = null;

	private ElementHasher hasher = null;

	private long segmentSize = 0;

	private int keyLength = 0;

	private long valueLength = 0;

	private Lock createTableLock = new ReentrantLock();

	private SegmentArray table = null;

	private SyncSize size = null;

	private ResourceLocker<Long> locker = new ResourceLocker<Long>();

	public RawTable(BytePool pool,
			ElementEqualityComparator comparator,
			ElementHasher hasher,
			long segmentSize,
			int keyLength,
			long valueLength)
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

		this.pool = new TolerantBytePool(pool);
		this.comparator = comparator;
		this.hasher = hasher;
		this.segmentSize = Common.valueToExpValue(segmentSize);
		this.keyLength = keyLength;
		this.valueLength = valueLength;
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

	@Override
	public long size()
	{
		return this.size.get();
	}

	public long tableSize() throws IOException
	{
		return this.getTable().size();
	}

	private void createTable(int initSegNum) throws IOException
	{
		assert (null == table);

		long segNum = initSegNum;
		segNum = Math.max(2, segNum);
		segNum = Common.valueToExpValue(segNum);
		segNum = Math.min(segNum, Common.MAX_INT_FROM_EXP);
		assert (segNum < Integer.MAX_VALUE);
		assert (Common.valueToExpValue(segNum) == segNum);

		this.table = new SegmentArray(pool.getPool(), Common.LONG_SIZE,
				this.segmentSize);
		this.table.create((int) segNum);
		assert (this.table.size() >= 4);
		assert (this.table.size() == segNum * this.segmentSize);

		this.size.set(0);
		// TODO�� ����������BytePool.NULL==-1
		this.table.fill((byte) BytePool.NULL);
	}

	private boolean tryCreateTable(int initSegNum) throws IOException
	{
		if (null == this.table)
		{
			this.createTableLock.lock();
			try
			{
				if (null == this.table)
				{
					createTable(initSegNum);
					return true;
				}
			}
			finally
			{
				this.createTableLock.unlock();
			}
		}

		return false;
	}

	private void deleteTable() throws IOException
	{
		this.table.clear();
		this.table = null;
		this.size.set(0);
	}

	private SegmentArray getTable() throws IOException
	{
		tryCreateTable(2);
		return this.table;
	}

	// TODO clear������Ҫȫ�ֶ�д����д���ı���
	@Override
	public void clear() throws IOException
	{
		for (long idx = 0; idx < tableSize(); idx++)
		{
			SlotIterator itr = this.slotIterator(idx);
			while (itr.hasNext())
			{
				itr.next();
				itr.remove();
			}
		}
		this.deleteTable();
	}

	@Override
	public void create(Object... args) throws IOException
	{
		this.size = new SyncSize(0);
		this.createTable((args != null && args[0] instanceof Integer) ? (Integer) args[0]
				: 2);
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		this.table = new SegmentArray(pool.getPool(), Common.LONG_SIZE,
				this.segmentSize);
		this.table.read(in);

		assert (Common.valueToExpValue(this.table.getSegmentSize()) == this.table
				.getSegmentSize());
		while (Common.valueToExpValue(this.table.getSegmentNum()) != this.table
				.getSegmentNum())
		{
			// TODO ���Ժ�ɾȥ
			System.err
					.println("RawTable.write(): table segment lost, repairing now...");
			this.table.addSegment();
		}

		long size = in.readLong();
		size = Math.max(0, size);
		this.size = new SyncSize(size);
	}

	// TODO write������Ҫȫ��д���ı���
	@Override
	public void write(DataOutput out) throws IOException
	{
		getTable().write(out);
		out.writeLong(size.get());
	}

	private long getSlot(long idx) throws IOException
	{
		assert (getTable().getElementLength() == Common.LONG_SIZE);
		byte[] buf = new byte[Common.LONG_SIZE];
		getTable().getElement(idx, buf, 0, 0, Common.LONG_SIZE);
		return Common.byteToLong(buf, 0);
	}

	private void setSlot(long idx, long value) throws IOException
	{
		assert (getTable().getElementLength() == Common.LONG_SIZE);
		byte[] buf = new byte[Common.LONG_SIZE];
		Common.longToByte(buf, 0, value);
		getTable().setElement(idx, buf, 0, 0, Common.LONG_SIZE);
	}

	public void expandTo(int segNum) throws IOException
	{
		if (segNum < 0
				|| segNum > Common.MAX_INT_FROM_EXP
				|| Common.valueToExpValue(segNum) != segNum)
		{
			throw new IllegalArgumentException("illegal segNum");
		}

		while (getTable().getSegmentNum() < segNum)
		{
			getTable().addSegment();
		}
		assert (getTable().getSegmentNum() == segNum);
	}

	// TODO: ��java.util.HashMap�Ĺ�ϣ��������֪���µĶԲ��ԣ���Ҫ���ԡ�
	// �����ϣ�㷨��Ŀ�����ڻ�ϸ�λ�͵�λ�ı��أ���Դ�СΪ2���ݴεı�
	// ���͹�ϣֵ���λ��ͬ�������䵽���ͬһ���еĸ���
	// ���������ȡ�Ԫ�طֲ�
	private long hash(long h)
	{
		h ^= (h >>> 49) ^ (h >>> 32);
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	public ElementViewer keyToViewer(final byte[] keyBuf, final int keyBufOffset)
	{
		if (null == keyBuf)
		{
			throw new NullPointerException("keyBuf is null");
		}
		if (keyBufOffset < 0
				|| keyBufOffset + this.getKeyLength() > keyBuf.length)
		{
			throw new IndexOutOfBoundsException("keyBufOffset illegal");
		}

		return new ElementViewer()
		{

			@Override
			public void get(byte[] buf,
					int bufOffset,
					long idxInElement,
					int length) throws IOException
			{
				if (idxInElement < 0
						|| length < 0
						|| idxInElement + length > getKeyLength())
				{
					throw new IndexOutOfBoundsException(
							"idxInElement or length is out of bounds");
				}

				System.arraycopy(keyBuf,
						keyBufOffset + (int) idxInElement,
						buf,
						bufOffset,
						length);
			}

		};
	}

	public long keyToHash(ElementViewer keyViewer) throws IOException
	{
		if (null == keyViewer)
		{
			throw new NullPointerException("null keyViewer");
		}

		return hash(this.hasher.hash(keyViewer));
	}

	// TODO ����Ƿ�����hash�������·�жϣ�����key��ȱȽ�
	public boolean keyEquals(ElementViewer keyViewerA, ElementViewer keyViewerB)
			throws IOException
	{
		if (null == keyViewerA || null == keyViewerB)
		{
			throw new NullPointerException("null keyViewer");
		}

		return this.comparator.equals(keyViewerA, keyViewerB);
	}

	public Node createNode(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBuf,
			int valueBufOffset,
			long valueIdxInElement,
			int valueLength,
			long hash) throws IOException
	{
		Node node = new Node(pool.alloc(getNodeLength()));
		try
		{
			node.setHash(hash);
			node.setKey(keyBuf, keyBufOffset);
			node.setValue(valueBuf,
					valueBufOffset,
					valueIdxInElement,
					valueLength);

			return node;
		}
		catch (RuntimeException ex)
		{
			this.deleteNode(node);
			throw ex;
		}
		catch (IOException ex)
		{
			this.deleteNode(node);
			throw ex;
		}
	}

	public boolean deleteNode(Node node) throws IOException
	{
		if (null == node)
		{
			throw new NullPointerException("null node");
		}

		return this.pool.free(node.getKey());
	}

	public long indexForHash(long hash, long tableSize)
	{
		// Node ��������ܻ����������ʹ��λ�������index��ȡ�õ���������
		if (Common.valueToExpValue(tableSize) != tableSize)
		{
			throw new IllegalArgumentException("tableSize illegal");
		}

		return hash & (tableSize - 1);
	}

	// TODO ʹ��ǰӦ��lock��idx��slot
	// TODO ���쳡�����ر���ɢ�е�ͬһ��slot�Ķ��Ԫ�أ����ܷ�����ʧ�������
	public void add(long idx, Node node) throws IOException
	{
		if (idx < 0 || idx >= tableSize())
		{
			throw new IllegalArgumentException("illegal idx");
		}
		if (null == node)
		{
			throw new NullPointerException("null node");
		}
		boolean succ = node.setNext(this.getSlot(idx));
		if (!succ)
		{
			throw new IllegalArgumentException("node does not exist");
		}

		this.setSlot(idx, node.getKey());
		size.incr();
	}

	// ��������ʹ��ʱ���Ƿ�©����lock slot
	public void lockSlot(long idx)
	{
		this.locker.lock(idx);
	}

	public void unlockSlot(long idx)
	{
		this.locker.unlock(idx);
	}

	// TODO ʹ��ǰӦ��lock��idx��slot
	public SlotIterator slotIterator(long slotIdx) throws IOException
	{
		return new SlotIterator(slotIdx);
	}

	private long getNodeLength()
	{
		return Common.LONG_SIZE
				* 2
				+ this.getKeyLength()
				+ this.getValueLength();
	}

	public class Node
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

		public boolean setHash(long hash) throws IOException
		{
			byte[] buf = new byte[Common.LONG_SIZE];
			Common.longToByte(buf, 0, hash);
			return pool.set(key, buf, Common.LONG_SIZE);
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

		public boolean setKey(byte[] buf, int bufOffset) throws IOException
		{
			return pool.set(key,
					buf,
					bufOffset,
					2 * Common.LONG_SIZE,
					getKeyLength());
		}

		public boolean getValue(byte[] buf,
				int bufOffset,
				long idxInElement,
				int length) throws IOException
		{
			if (idxInElement < 0
					|| length < 0
					|| length + idxInElement > getValueLength())
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
					|| length + idxInElement > getValueLength())
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

		public boolean exists() throws IOException
		{
			return this.key >= 0
					&& this.getValue(new byte[1], 0, getValueLength() - 1, 1);
		}
	}

	//TODO ��Common.Iterator�Ľӿڲ���
	public class SlotIterator
	{
		private long slotIdx = 0;

		private Node next = null;

		private Node prev = null;

		private Node prevPrev = null;

		public SlotIterator(long idx) throws IOException
		{
			if (idx < 0 || idx >= tableSize())
			{
				throw new IllegalArgumentException("illegal idx");
			}

			this.slotIdx = idx;
			this.next = new Node(RawTable.this.getSlot(idx));
			this.prev = null;
			this.prevPrev = null;
		}

		public boolean hasNext() throws IOException
		{
			return this.next.exists();
		}

		public Node next() throws IOException
		{
			Node ret = next;

			long[] nextKey = new long[1];
			boolean succ = next.getNext(nextKey);
			if (!succ)
			{
				throw new NoSuchElementException();
			}

			prevPrev = prev != null ? prev : prevPrev;
			prev = next;
			next = new Node(nextKey[0]);

			return ret;
		}

		// TODO С�Ĳ���ɾ�������ڸ�������£�����nextΪ����ĵ�һ��Ԫ�أ���itr��û�ж�����������Ԫ�����ȵȣ��Ƿ��ܹ���������
		// ��������ɾ����һ��һɾ
		public void remove() throws IOException
		{
			if (null == prev)
			{
				throw new NoSuchElementException();
			}

			if (this.prevPrev != null)
			{
				this.prevPrev.setNext(this.next.getKey());
			}
			else
			{
				RawTable.this.setSlot(slotIdx, this.next.getKey());
			}

			Node tobeRemoved = this.prev;
			this.prev = null;
			size.decr();

			RawTable.this.deleteNode(tobeRemoved);
		}
	}

}
