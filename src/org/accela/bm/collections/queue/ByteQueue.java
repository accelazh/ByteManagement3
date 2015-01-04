package org.accela.bm.collections.queue;

import java.io.IOException;

import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementAccesser;
import org.accela.bm.collections.common.ElementContainer;
import org.accela.bm.common.Persistancer;

//双头队列，可以作为队列或者栈来使用。
public interface ByteQueue extends Persistancer, ElementContainer
{
	// TODO 哈希表，以及以后的数据结构别忘了这个接口Clearable，及其特殊意义
	// 这个量对于queue实例，应当是终生不变的
	public void addHead(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	public void addTail(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	public boolean removeHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public boolean removeTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public void get(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public void set(long idx,
			byte[] bufNew,
			int bufOffsetNew,
			long idxInElementNew,
			int lengthNew,
			byte[] bufOld,
			int bufOffsetOld,
			long idxInElementOld,
			int lengthOld) throws IOException;

	public boolean getHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public boolean getTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public ByteIterator<ElementAccesser> iterator(long idx) throws IOException;

	public ByteIterator<ElementAccesser> iterator() throws IOException;
}
