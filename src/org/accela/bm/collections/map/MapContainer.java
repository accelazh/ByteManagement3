package org.accela.bm.collections.map;

import org.accela.bm.common.Clearable;
import org.accela.bm.common.Sizable;

public interface MapContainer extends Clearable, Sizable
{
	public int getKeyLength();

	public long getValueLength();
}
