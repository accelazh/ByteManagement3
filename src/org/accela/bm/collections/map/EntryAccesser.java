package org.accela.bm.collections.map;

import org.accela.bm.collections.common.ElementAccesser;
import org.accela.bm.collections.common.ElementViewer;

public interface EntryAccesser
{
	public ElementViewer getKey();

	public ElementAccesser getValue();
	
}
