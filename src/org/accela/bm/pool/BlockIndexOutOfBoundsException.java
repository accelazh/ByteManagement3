package org.accela.bm.pool;

//@see BytePool
public class BlockIndexOutOfBoundsException extends IndexOutOfBoundsException
{
	private static final long serialVersionUID = 1L;

	public BlockIndexOutOfBoundsException()
	{
		super();
	}

	public BlockIndexOutOfBoundsException(String message)
	{
		super(message);
	}
}
