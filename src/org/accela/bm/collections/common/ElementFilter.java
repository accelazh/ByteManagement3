package org.accela.bm.collections.common;

import java.io.IOException;

public interface ElementFilter
{
	public boolean accept(ElementViewer element) throws IOException;
}
