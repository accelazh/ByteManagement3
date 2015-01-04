package org.accela.bm.pool;

import java.io.IOException;

public interface ByteAccesserFactory
{
	public ByteAccesser create() throws IOException;
}
