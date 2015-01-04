package org.accela.bm.collections.queue.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementAccesser;
import org.accela.bm.collections.queue.ByteQueue;
import org.accela.bm.common.Concurrentable;
import org.accela.bm.common.DataFormatException;

public class SynchronizedByteQueue implements ByteQueue, Concurrentable
{
	private ByteQueue queue = null;

	public SynchronizedByteQueue(ByteQueue queue)
	{
		if (null == queue)
		{
			throw new IllegalArgumentException("queue should not be null");
		}

		this.queue = queue;
	}

	public ByteQueue getQueue()
	{
		return this.queue;
	}

	@Override
	public long getElementLength()
	{
		return queue.getElementLength();
	}

	@Override
	public synchronized void addHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		queue.addHead(buf, bufOffset, idxInElement, length);
	}

	@Override
	public synchronized void addTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		queue.addTail(buf, bufOffset, idxInElement, length);
	}

	@Override
	public synchronized void create(Object... args) throws IOException
	{
		queue.create(args);
	}

	@Override
	public synchronized void get(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		queue.get(idx, buf, bufOffset, idxInElement, length);
	}

	@Override
	public synchronized boolean getHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		return queue.getHead(buf, bufOffset, idxInElement, length);
	}

	@Override
	public synchronized boolean getTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		return queue.getTail(buf, bufOffset, idxInElement, length);
	}

	@Override
	public synchronized void read(DataInput in) throws IOException,
			DataFormatException
	{
		queue.read(in);
	}

	@Override
	public synchronized boolean removeHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		return queue.removeHead(buf, bufOffset, idxInElement, length);
	}

	@Override
	public synchronized boolean removeTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		return queue.removeTail(buf, bufOffset, idxInElement, length);
	}

	@Override
	public synchronized void set(long idx,
			byte[] bufNew,
			int bufOffsetNew,
			long idxInElementNew,
			int lengthNew,
			byte[] bufOld,
			int bufOffsetOld,
			long idxInElementOld,
			int lengthOld) throws IOException
	{
		queue.set(idx,
				bufNew,
				bufOffsetNew,
				idxInElementNew,
				lengthNew,
				bufOld,
				bufOffsetOld,
				idxInElementOld,
				lengthOld);
	}

	@Override
	public synchronized long size()
	{
		return queue.size();
	}

	@Override
	public synchronized void write(DataOutput out) throws IOException
	{
		queue.write(out);
	}

	@Override
	public synchronized void clear() throws IOException
	{
		queue.clear();
	}

	@Override
	public ByteIterator<ElementAccesser> iterator(long idx) throws IOException
	{
		return queue.iterator(idx);
	}

	@Override
	public ByteIterator<ElementAccesser> iterator() throws IOException
	{
		return queue.iterator();
	}

}
