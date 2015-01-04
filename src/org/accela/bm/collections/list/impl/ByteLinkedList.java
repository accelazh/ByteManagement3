package org.accela.bm.collections.list.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.accela.bm.collections.common.ElementAccesser;
import org.accela.bm.collections.list.ByteListBrokenException;
import org.accela.bm.collections.list.ByteListIterator;
import org.accela.bm.common.Common;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.pool.BytePool;
import org.accela.bm.pool.TolerantBytePool;

public class ByteLinkedList extends AbstractByteList
{
	private TolerantBytePool pool = null;

	private long elementLength = 0;

	private long size = 0;

	private Lock createListLock = new ReentrantLock();

	private Node head = null;

	private Node tail = null;

	private transient long modCount = 0;

	public ByteLinkedList(BytePool pool, long elementLength)
	{
		if (null == pool)
		{
			throw new IllegalArgumentException("null pool");
		}
		if (elementLength < 1)
		{
			throw new IllegalArgumentException("illegal elementLength: "
					+ elementLength);
		}

		this.pool = new TolerantBytePool(pool);
		this.elementLength = elementLength;
	}

	public BytePool getPool()
	{
		return pool.getPool();
	}

	@Override
	public long getElementLength()
	{
		return this.elementLength;
	}

	@Override
	public long size()
	{
		return this.size;
	}

	private void createList() throws IOException
	{
		this.head = new Node(pool.alloc(getNodeLength()));
		this.tail = new Node(pool.alloc(getNodeLength()));
		this.head.setNext(this.tail.getKey());
		this.head.setPrev(BytePool.NULL);
		this.tail.setNext(BytePool.NULL);
		this.tail.setPrev(this.head.getKey());

		this.size = 0;
	}

	private boolean tryCreateList() throws IOException
	{
		if (null == this.head || null == this.tail)
		{
			createListLock.lock();
			try
			{
				if (null == this.head || null == this.tail)
				{
					createList();
					return true;
				}
			}
			finally
			{
				createListLock.unlock();
			}
		}

		return false;
	}

	private void deleteList() throws IOException
	{
		super.clear();
		this.size = 0;

		if (this.head != null)
		{
			pool.free(this.head.getKey());
		}
		if (this.tail != null)
		{
			pool.free(this.tail.getKey());
		}
	}

	@Override
	public void clear() throws IOException
	{
		super.clear();
		deleteList();
	}

	@Override
	public void create(Object... args) throws IOException
	{
		head = null;
		tail = null;
		size = 0;
		modCount = 0;
	}

	// TODO 尝试空链表持久化，和清空后的链表的持久化
	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		long size = in.readLong();
		long head = in.readLong();
		long tail = in.readLong();

		if (head <= BytePool.NULL || tail <= BytePool.NULL)
		{
			head = BytePool.NULL;
			tail = BytePool.NULL;
			size = 0;
		}

		this.head = BytePool.NULL == head ? null : new Node(head);
		this.tail = BytePool.NULL == tail ? null : new Node(tail);
		this.size = size;
		this.modCount = 0;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(size);
		out.writeLong(null == head ? BytePool.NULL : head.getKey());
		out.writeLong(null == tail ? BytePool.NULL : tail.getKey());
	}

	@Override
	public ByteListIterator iterator(long idx) throws IOException
	{
		if (idx < 0 || idx > size())
		{
			throw new IndexOutOfBoundsException("idx: " + idx);
		}

		tryCreateList();

		ByteListIterator itr = null;
		if (idx <= size() / 2)
		{
			itr = new NodeIterator(head, -1);
		}
		else
		{
			itr = new NodeIterator(tail, size);
		}

		while (itr.index() > idx && itr.hasCur())
		{
			itr.backward();
		}
		while (itr.index() < idx && itr.hasCur())
		{
			itr.forward();
		}

		if (itr.index() != idx)
		{
			throw new ByteListBrokenException("can't move to idx: " + idx);
		}

		return itr;
	}

	private long getNodeLength()
	{
		return 2 * Common.LONG_SIZE + getElementLength();
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

		public boolean getPrev(long[] prev) throws IOException
		{
			byte[] buf = new byte[Common.LONG_SIZE];
			boolean succ = pool.get(key, buf, Common.LONG_SIZE);
			if (succ)
			{
				prev[0] = Common.byteToLong(buf, 0);
			}
			return succ;
		}

		public boolean setPrev(long prev) throws IOException
		{
			byte[] buf = new byte[Common.LONG_SIZE];
			Common.longToByte(buf, 0, prev);
			return pool.set(key, buf, Common.LONG_SIZE);
		}

		public boolean getElement(byte[] buf,
				int bufOffset,
				long idxInElement,
				int length) throws IOException
		{
			if (idxInElement < 0
					|| length < 0
					|| length + idxInElement > getElementLength())
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

		public boolean setElement(byte[] buf,
				int bufOffset,
				long idxInElement,
				int length) throws IOException
		{
			if (idxInElement < 0
					|| length < 0
					|| length + idxInElement > getElementLength())
			{
				throw new IndexOutOfBoundsException(
						"idxInElement or length illegal"
								+ idxInElement
								+ ", "
								+ length);
			}

			return pool.set(key, buf, bufOffset, 2
					* Common.LONG_SIZE
					+ idxInElement, length);
		}

		public boolean exists() throws IOException
		{
			return this.key >= 0
					&& this.getElement(new byte[1],
							0,
							getElementLength() - 1,
							1);
		}

	}// end of class Node

	private class NodeElementAccesser implements ElementAccesser
	{
		private Node node = null;

		public NodeElementAccesser(Node node)
		{
			assert (node != null);
			this.node = node;
		}

		@Override
		public void get(byte[] buf, int bufOffset, long idxInElement, int length)
				throws IOException
		{
			boolean succ = node
					.getElement(buf, bufOffset, idxInElement, length);
			if (!succ)
			{
				throw new NoSuchElementException();
			}
		}

		@Override
		public void set(byte[] bufNew,
				int bufOffsetNew,
				long idxInElementNew,
				int lengthNew,
				byte[] bufOld,
				int bufOffsetOld,
				long idxInElementOld,
				int lengthOld) throws IOException
		{
			boolean succ = true;
			if (bufOld != null)
			{
				succ = succ
						&& node.getElement(bufOld,
								bufOffsetOld,
								idxInElementOld,
								lengthOld);
			}
			succ = succ
					&& node.setElement(bufNew,
							bufOffsetNew,
							idxInElementNew,
							lengthNew);
			if (!succ)
			{
				throw new NoSuchElementException();
			}
		}
	}

	private class NodeIterator implements ByteListIterator
	{
		private Node cur = null;

		private long index = 0; // TODO 检查idx==list.size()和idx==-1的点

		private long localModCount = 0; // TODO

		// TODO 测试ConcurrentModificationException
		public NodeIterator(Node cur, long initIdx)
		{
			if (null == cur)
			{
				throw new IllegalArgumentException("null cur");
			}

			this.cur = cur;
			this.index = initIdx;
			this.localModCount = 0;
		}

		private void testModCount()
		{
			if (localModCount != modCount)
			{
				throw new ConcurrentModificationException();
			}
		}

		private void addModCount()
		{
			localModCount++;
			modCount++;
		}

		private Node getNode(long key)
		{
			return new Node(key);
		}

		private boolean isFakeHead(Node node)
		{
			return node.getKey() == head.getKey();
		}

		private boolean isFakeTail(Node node)
		{
			return node.getKey() == tail.getKey();
		}

		private boolean isFake(Node node)
		{
			assert (node != null);
			return isFakeHead(node) || isFakeTail(node);
		}

		private boolean isExist(Node node) throws IOException
		{
			assert (node != null);
			return !isFake(node) && node.exists();
		}

		@Override
		public boolean hasCur() throws IOException
		{
			testModCount();
			return isExist(cur);
		}

		// TODO 检查在head和tail时，是否能回到有效结点上
		@Override
		public void forward() throws IOException
		{
			testModCount();
			if (isFakeTail(cur))
			{
				throw new NoSuchElementException();
			}

			long[] next = new long[1];
			boolean succ = cur.getNext(next);
			if (!succ)
			{
				throw new NoSuchElementException();
			}
			this.cur = getNode(next[0]);
			index++;
		}

		@Override
		public void backward() throws IOException
		{
			testModCount();
			if (isFakeHead(cur))
			{
				throw new NoSuchElementException();
			}

			long[] prev = new long[1];
			boolean succ = cur.getPrev(prev);
			if (!succ)
			{
				throw new NoSuchElementException();
			}
			this.cur = getNode(prev[0]);
			index--;
		}

		@Override
		public ElementAccesser get() throws IOException
		{
			testModCount();
			if (isFake(cur))
			{
				throw new NoSuchElementException();
			}

			return new NodeElementAccesser(cur);
		}

		@Override
		public long index()
		{
			testModCount();
			return index;
		}

		// TODO 测试-1点不能加node
		@Override
		public void add(byte[] buf, int bufOffset, long idxInElement, int length)
				throws IOException
		{
			testModCount();
			addModCount();
			if (isFakeHead(cur))
			{
				throw new NoSuchElementException();
			}

			Node newNode = new Node(pool.alloc(getNodeLength()));
			newNode.setElement(buf, bufOffset, idxInElement, length);

			final Node curNode = cur;
			long[] prev = new long[1];
			boolean succ = curNode.getPrev(prev);
			if (!succ)
			{
				throw new NoSuchElementException();
			}

			newNode.setNext(cur.getKey());
			newNode.setPrev(prev[0]);
			curNode.setPrev(newNode.getKey());
			getNode(prev[0]).setNext(newNode.getKey());
			size++;
		}

		@Override
		public void remove() throws IOException
		{
			testModCount();
			addModCount();
			if (isFake(cur))
			{
				throw new NoSuchElementException();
			}

			final Node curNode = cur;
			long[] next = new long[1];
			boolean succ = curNode.getNext(next);
			long[] prev = new long[1];
			succ = succ && curNode.getPrev(prev);
			if (!succ)
			{
				throw new NoSuchElementException();
			}

			final Node nextNode = getNode(next[0]);
			nextNode.setPrev(prev[0]);
			final Node prevNode = getNode(prev[0]);
			prevNode.setNext(next[0]);

			// TODO 测试删除单元素链，测试remove/add/set后继续迭代
			// TODO 测试多次add/remove/set，以及交叉迭代与add/remove/set
			cur = nextNode;
			size -= Math.min(1, size);
			pool.free(curNode.getKey());
		}

	}
}
