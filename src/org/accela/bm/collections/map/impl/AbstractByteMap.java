package org.accela.bm.collections.map.impl;

import java.io.IOException;

import org.accela.bm.collections.map.ByteMap;

public abstract class AbstractByteMap implements ByteMap
{
	public boolean containsKey(byte[] keyBuf, int keyBufOffset)
			throws IOException
	{
		return this.get(keyBuf, keyBufOffset, new byte[1], 0, 0, 0);
	}

}
