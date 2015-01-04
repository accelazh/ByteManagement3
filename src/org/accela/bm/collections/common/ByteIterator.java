package org.accela.bm.collections.common;

import java.io.IOException;

public interface ByteIterator<T>
{
	public boolean hasCur() throws IOException;

	// move to the next element or position
	public void forward() throws IOException;

	// get the element the iterator currently points to
	public T get() throws IOException;

	// remove the element the iterator currently points to. after that
	// the iterator points to the next element or position (index++).
	public void remove() throws IOException;
}
