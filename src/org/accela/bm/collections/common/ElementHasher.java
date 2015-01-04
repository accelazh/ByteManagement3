package org.accela.bm.collections.common;

import java.io.IOException;

public interface ElementHasher
{
	// Note:
	// ElementEqualityComparator判断相等的元素，hash也应该相等。
	// hash相等的元素，ElementEqualityComparator可以不相等，但最好相等。
	// 一个元素的哈希码，应该是终身保持不变的
	public long hash(ElementViewer element) throws IOException;
}
