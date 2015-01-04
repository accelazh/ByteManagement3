package org.accela.bm.pool.test;

import java.io.IOException;

import org.accela.bm.pool.impl.RafByteAccesser;

public class TestRafByteAccesser extends TestByteAccesser
{
	@Override
	protected void open(Object... args) throws IOException
	{
		this.accesser = new RafByteAccesser(testFile);
	}

}
