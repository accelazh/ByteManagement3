package org.accela.bm.collections.common;

import java.io.IOException;

public interface ElementViewer
{
	public void get(byte[] buf, int bufOffset, long idxInElement, int length)
	throws IOException;
}
