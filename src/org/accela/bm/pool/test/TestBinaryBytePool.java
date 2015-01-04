package org.accela.bm.pool.test;

import java.io.IOException;

import org.accela.bm.common.BytePersistancer;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.ByteAccesserFactory;
import org.accela.bm.pool.impl.ConcurrentByteAccesser;
import org.accela.bm.pool.impl.RafByteAccesser;
import org.accela.bm.pool.impl.binaryPool.BinaryBytePool;

public class TestBinaryBytePool extends TestBytePool
{
	@Override
	protected void open(Object... args) throws IOException
	{
		this.accesser = new ConcurrentByteAccesser(16,
				new ByteAccesserFactory()
				{

					@Override
					public ByteAccesser create() throws IOException
					{
						return new RafByteAccesser(testFile);
					}
				});

		byte[] data = null;
		if (this.pool != null)
		{
			data = new BytePersistancer(this.pool).writeBytes();
		}
		this.pool = new BinaryBytePool(accesser);
		if (data != null && Boolean.TRUE == args[0])
		{
			try
			{
				new BytePersistancer(this.pool).readBytes(data);
			}
			catch (DataFormatException ex)
			{
				ex.printStackTrace();
				assert (false);
			}
		}
		else
		{
			this.pool.create();
		}
	}

}
