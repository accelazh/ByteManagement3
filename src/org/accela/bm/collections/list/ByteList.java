package org.accela.bm.collections.list;

import java.io.IOException;

import org.accela.bm.collections.common.ElementContainer;
import org.accela.bm.collections.common.ElementFilter;
import org.accela.bm.collections.queue.ByteQueue;
import org.accela.bm.common.Persistancer;

public interface ByteList extends ByteQueue, Persistancer, ElementContainer
{
	public void insert(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public void remove(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	@Override
	public void set(long idx,
			byte[] bufNew,
			int bufOffsetNew,
			long idxInElementNew,
			int lengthNew,
			byte[] bufOld,
			int bufOffsetOld,
			long idxInElementOld,
			int lengthOld) throws IOException;

	@Override
	public void get(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	// startIdx inclusive
	public long indexOf(long startIdx, ElementFilter comparator)
			throws IOException;

	// endIdx inclusive
	public long lastIndexOf(long endIdx, ElementFilter comparator)
			throws IOException;

	@Override
	public ByteListIterator iterator(long idx) throws IOException;

	// ======================================================================

	@Override
	public void addHead(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	@Override
	public void addTail(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	public boolean contains(ElementFilter comparator) throws IOException;

	public boolean remove(ElementFilter comparator,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public long indexOf(ElementFilter comparator) throws IOException;

	public long lastIndexOf(ElementFilter comparator) throws IOException;

	@Override
	public ByteListIterator iterator() throws IOException;

}
