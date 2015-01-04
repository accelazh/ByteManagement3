package org.accela.bm.collections.common;

import java.io.IOException;

public interface ElementComparator extends ElementEqualityComparator
{
	public int compare(ElementViewer elementA, ElementViewer elementB)
			throws IOException;
}
