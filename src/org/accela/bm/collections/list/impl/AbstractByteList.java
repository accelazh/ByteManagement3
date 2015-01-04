package org.accela.bm.collections.list.impl;

import java.io.IOException;

import org.accela.bm.collections.common.ElementFilter;
import org.accela.bm.collections.list.ByteList;
import org.accela.bm.collections.list.ByteListIterator;

public abstract class AbstractByteList implements ByteList
{
	@Override
	public void addHead(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException
	{
		this.insert(0, buf, bufOffset, idxInElement, length);
	}

	@Override
	public void addTail(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException
	{
		this.insert(this.size(), buf, bufOffset, idxInElement, length);
	}

	@Override
	public boolean contains(ElementFilter comparator) throws IOException
	{
		return indexOf(comparator) >= 0;
	}

	@Override
	public long indexOf(ElementFilter comparator) throws IOException
	{
		return indexOf(0, comparator);
	}

	@Override
	public ByteListIterator iterator() throws IOException
	{
		return iterator(0);
	}

	@Override
	public long lastIndexOf(ElementFilter comparator) throws IOException
	{
		return lastIndexOf(size() - 1, comparator);
	}

	@Override
	public boolean remove(ElementFilter comparator,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		long idx = indexOf(comparator);
		if (idx >= 0)
		{
			this.remove(idx, buf, bufOffset, idxInElement, length);
			return true;
		}
		return false;
	}

	@Override
	public boolean getHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size() <= 0)
		{
			return false;
		}
		this.get(0, buf, bufOffset, idxInElement, length);
		return true;
	}

	@Override
	public boolean getTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size() <= 0)
		{
			return false;
		}
		this.get(size() - 1, buf, bufOffset, idxInElement, length);
		return true;
	}

	@Override
	public boolean removeHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size() <= 0)
		{
			return false;
		}
		this.remove(0, buf, bufOffset, idxInElement, length);
		return true;
	}

	@Override
	public boolean removeTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (size() <= 0)
		{
			return false;
		}
		this.remove(size() - 1, buf, bufOffset, idxInElement, length);
		return true;
	}

	// TODO 一定要小心，Iterator的get返回的ElementAccesser，不能在Iterator步进后，被改变成访问别的元素
	@Override
	public void get(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		// TODO 检查是否在idx越界时正确抛出异常，下同
		ByteListIterator itr = iterator(idx);
		// TODO itr.get应该检查hasCur
		itr.get().get(buf, bufOffset, idxInElement, length);
	}

	@Override
	public long indexOf(long startIdx, ElementFilter comparator)
			throws IOException
	{
		// TODO 测试是否能够成功移动到链表尾部及头部，再移动会移不动，并且能够成功添加头、尾鱼元素
		if (null == comparator)
		{
			throw new IllegalArgumentException("null comparator");
		}

		final ByteListIterator itr = iterator(startIdx);
		while (itr.hasCur())
		{
			boolean accept = comparator.accept(itr.get());
			if (accept)
			{
				return itr.index();
			}
			itr.forward();
		}
		return -1;
	}

	@Override
	public long lastIndexOf(long endIdx, ElementFilter comparator)
			throws IOException
	{
		if (null == comparator)
		{
			throw new IllegalArgumentException("null comparator");
		}

		final ByteListIterator itr = iterator(endIdx);
		while (itr.hasCur())
		{
			boolean accept = comparator.accept(itr.get());
			if (accept)
			{
				return itr.index();
			}
			itr.backward();
		}
		return -1;
	}

	@Override
	public void insert(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		ByteListIterator itr = this.iterator(idx);
		itr.add(buf, bufOffset, idxInElement, length);
	}

	@Override
	public void remove(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		ByteListIterator itr = this.iterator(idx);
		itr.remove();
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
		ByteListIterator itr = this.iterator(idx);
		itr.get().set(bufNew,
				bufOffsetNew,
				idxInElementNew,
				lengthNew,
				bufOld,
				bufOffsetOld,
				idxInElementOld,
				lengthOld);
	}

	// Note: 子类可能需要继续添加内容，以释放掉其它空间
	@Override
	public void clear() throws IOException
	{
		ByteListIterator itr = this.iterator(0);
		while (itr.hasCur())
		{
			itr.remove();
		}

		assert (size() == 0);
		// TODO 子类可能需要添加内容
	}

}
