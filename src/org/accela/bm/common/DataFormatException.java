package org.accela.bm.common;

//TODO ���տ���Ϊ�˷��㣬����̳�IOException
public class DataFormatException extends Exception
{
	private static final long serialVersionUID = 1L;

	public DataFormatException()
	{
		super();
	}

	public DataFormatException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public DataFormatException(String message)
	{
		super(message);
	}

	public DataFormatException(Throwable cause)
	{
		super(cause);
	}

}
