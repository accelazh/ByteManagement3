package org.accela.bm.collections.queue.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.accela.bm.collections.array.SegmentArray;
import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementAccesser;
import org.accela.bm.collections.queue.ByteQueue;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.pool.BytePool;

public class SegmentQueue implements ByteQueue
{
	private SegmentArray array = null;

	private long headIdx = 0;

	private long size = 0;

	private BytePool pool = null;

	private long elementLength = 0;

	private long segmentSize = 0;

	public SegmentQueue(BytePool pool, long elementLength, long segmentSize)
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
		if (segmentSize < 1)
		{
			throw new IllegalArgumentException("illegal segmentSize: "
					+ segmentSize);
		}

		this.pool = pool;
		this.elementLength = elementLength;
		this.segmentSize = segmentSize;
	}

	public BytePool getPool()
	{
		return this.pool;
	}

	@Override
	public long getElementLength()
	{
		return this.elementLength;
	}

	public long getSegmentSize()
	{
		return this.segmentSize;
	}

	@Override
	public long size()
	{
		return this.size;
	}

	@Override
	public void create(Object... args) throws IOException
	{
		array = new SegmentArray(pool, elementLength, segmentSize);
		array.create(0); // TODO 测试是否新建了空Array
		headIdx = 0;
		size = 0;

		assert (checkValid());
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		long headIdx = in.readLong();
		long size = in.readLong();
		array = new SegmentArray(pool, elementLength, segmentSize);
		array.read(in);

		this.headIdx = Math.min(Math.max(0, headIdx), array.length());
		this.size = Math.min(Math.max(0, size), array.length() - headIdx);

		assert (checkValid());
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(headIdx);
		out.writeLong(size);
		array.write(out);
	}

	// ===============================================================

	private boolean checkValid()
	{
		if (headIdx < 0 || headIdx > array.length())
		{
			return false;
		}
		if (size < 0 || headIdx + size > array.length())
		{
			return false;
		}

		return true;
	}

	@Override
	public void addHead(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException
	{
		while (headIdx <= 0)
		{
			array.insertSegment(0);
			headIdx += array.getSegmentSize();
		}
		assert (headIdx > 0);

		array.setElement(headIdx - 1, buf, bufOffset, idxInElement, length);
		headIdx--;
		size++;

		assert (checkValid());
	}

	@Override
	public void addTail(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException
	{
		long elementIdx = headIdx + size;
		while (elementIdx >= array.length())
		{
			array.addSegment();
		}
		assert (array.length() > elementIdx);

		array.setElement(elementIdx, buf, bufOffset, idxInElement, length);
		size++;

		assert (checkValid());
	}

	@Override
	public boolean removeHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size <= 0)
		{
			return false;
		}

		array.getElement(headIdx, buf, bufOffset, idxInElement, length);
		headIdx++;
		size--;

		assert (checkValid());
		while (headIdx >= 2 * array.getSegmentSize()) // 乘以2是为了防止抖动
		{
			array.removeSegment(0);
			headIdx -= array.getSegmentSize();
		}

		assert (checkValid());
		return true;
	}

	@Override
	public boolean removeTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size <= 0)
		{
			return false;
		}

		array.getElement(headIdx + size - 1,
				buf,
				bufOffset,
				idxInElement,
				length);
		size--;

		assert (checkValid());
		while (array.length() - (headIdx + size) >= 2 * array.getSegmentSize()) // 乘以2是为了防止抖动
		{
			array.removeSegment(array.getSegmentNum() - 1);
		}
		assert (checkValid());

		return true;
	}

	@Override
	public void get(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (idx < 0 || idx >= size)
		{
			throw new IndexOutOfBoundsException("illegal idx: " + idx);
		}

		array.getElement(headIdx + idx, buf, bufOffset, idxInElement, length);
	}

	@Override
	public void set(long idx,
			byte[] bufNew,
			int bufOffsetNew,
			long idxInElementNew,
			int lengthNew,
			byte[] bufOld,
			int bufOffsetOld,
			long idxInElementOld,
			int lengthOld) throws IOException
	{
		if (idx < 0 || idx >= size)
		{
			throw new IndexOutOfBoundsException("illegal idx: " + idx);
		}

		if (bufOld != null)
		{
			array.getElement(headIdx + idx,
					bufOld,
					bufOffsetOld,
					idxInElementOld,
					lengthOld);
		}
		array.setElement(headIdx + idx,
				bufNew,
				bufOffsetNew,
				idxInElementNew,
				lengthNew);
	}

	@Override
	public boolean getHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size <= 0)
		{
			return false;
		}

		get(0, buf, bufOffset, idxInElement, length);
		return true;
	}

	@Override
	public boolean getTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size <= 0)
		{
			return false;
		}

		get(size - 1, buf, bufOffset, idxInElement, length);
		return true;
	}

	@Override
	public void clear() throws IOException
	{
		size = 0;
		headIdx = 0;

		array.clear();
	}

	@Override
	public ByteIterator<ElementAccesser> iterator(long idx) throws IOException
	{
		return new QueueIterator(idx);
	}

	@Override
	public ByteIterator<ElementAccesser> iterator() throws IOException
	{
		return this.iterator(0);
	}

	private class QueueElementAccesser implements ElementAccesser
	{
		private long idx = 0;

		public QueueElementAccesser(long idx)
		{
			this.idx = idx;
		}

		@Override
		public void get(byte[] buf, int bufOffset, long idxInElement, int length)
				throws IOException
		{
			SegmentQueue.this.get(idx, buf, bufOffset, idxInElement, length);
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
			SegmentQueue.this.set(idx,
					bufNew,
					bufOffsetNew,
					idxInElementNew,
					lengthNew,
					bufOld,
					bufOffsetOld,
					idxInElementOld,
					lengthOld);

		}
	}

	private class QueueIterator implements ByteIterator<ElementAccesser>
	{
		private long curIdx = 0;

		public QueueIterator(long curIdx)
		{
			this.curIdx = curIdx;
		}

		@Override
		public boolean hasCur() throws IOException
		{
			return curIdx >= 0 && curIdx < size();
		}

		@Override
		public void forward() throws IOException
		{
			curIdx++;
		}

		@Override
		public ElementAccesser get() throws IOException
		{
			return new QueueElementAccesser(curIdx);
		}

		@Override
		public void remove() throws IOException
		{
			throw new UnsupportedOperationException();
		}

	}

	// TODO 测试空间管理
	// TODO 测试临界大小，比如segmentSize==1、elementLength==1等
}
