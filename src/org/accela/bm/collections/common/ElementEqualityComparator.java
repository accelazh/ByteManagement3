package org.accela.bm.collections.common;

import java.io.IOException;

public interface ElementEqualityComparator
{
	public boolean equals(ElementViewer elementA, ElementViewer elementB)
			throws IOException;
}
