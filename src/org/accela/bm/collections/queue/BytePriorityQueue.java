package org.accela.bm.collections.queue;

import java.io.IOException;

import org.accela.bm.collections.common.ElementContainer;
import org.accela.bm.common.Persistancer;

//priority is a nonnegative integer, the smaller, the higher
public interface BytePriorityQueue extends Persistancer, ElementContainer
{
	public void enqueue(int priority,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	// 返回-1表示，所指元素不存在，下同
	public int dequeueHigh(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public int dequeueLow(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public boolean dequeueBy(int priority,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public int peekHigh(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	public int peekLow(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	public boolean peekBy(int priority,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public ByteQueue getQueue(int priority) throws IOException;
}
