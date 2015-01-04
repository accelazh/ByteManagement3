package org.accela.bm.collections.common;

import org.accela.bm.common.Clearable;
import org.accela.bm.common.Sizable;

public interface ElementContainer extends Clearable, Sizable
{
	public long getElementLength();
}
