package org.accela.bm.collections.list;

import java.io.IOException;

import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementAccesser;

/*
因为特殊的需要，我的迭代器和java.lang.Iterator及java.util.ListIterator
的设计方法略有不同。如下：
1. java.util.Iterator的next()函数包括前进和返回element，我的迭代器的前进forward
和后退backward函数仅仅管前进和后退，不返回element；get函数专管返回当前的element。
这样设计使得当迭代器停留在一个位置时，可以调用get函数多次返回element，而不引起迭代器
移动。当需要把迭代器从一个位置，移动到另一个位置时，调用forward和backward函数不会返回
element，引起取回中途的element的开销（磁盘文件中，这个开销不小）。
2. 为了迭代器的使用的简洁，以及能够使用add方法向链表尾部添加element，我的迭代器
没有hasNext和hasPrev方法，仅有一个hasCur方法判断当前指向的是否是一个合法的element。
如果hasCur返回true，那么调用forward和backward方法一定合法。以链表为例，这样使得
迭代器的index移动范围成了-1～list.size()（both inclusive）。当迭代器位于index=list.size()
时，调用add方法可以向表尾添加元素。
*/
public interface ByteListIterator extends ByteIterator<ElementAccesser>
{
	/*
	 * when this method returns true, it means the iterator currently points to
	 * an element. you can either invoke forward() of backward() at this time,
	 * both are legal. after that, the iterator may be moved to the next element
	 * or the previous element, or to the pos whose index==-1 or index==size().
	 * there are no elements on these positions, so hasCur() will return false
	 * then. in brief, the index range of the iterator is -1 ~ size() (both
	 * inclusive). when index==size(), you can invoke add() to add an element to
	 * the tail of the list.
	 */
	@Override
	public boolean hasCur() throws IOException;
	
	// move to the next element
	public void backward() throws IOException;
	
	// add an element to the pos that the iterator currently points to,
	// and shift the element previously at this pos and any subsequent elements
	// to the right. after add operation, the iterator points to the newly added
	// element.
	public void add(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;
	
	// return the index of the element the iterator currently points to
	public long index();
}
