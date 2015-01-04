package org.accela.bm.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

public class BytePersistanceDelegate<T> implements PersistanceDelegate<T>
{
	private PersistanceDelegate<T> delegate = null;

	public BytePersistanceDelegate(PersistanceDelegate<T> delegate)
	{
		if (null == delegate)
		{
			throw new IllegalArgumentException("delegate should not be null");
		}

		this.delegate = delegate;
	}

	public PersistanceDelegate<T> getDelegate()
	{
		return delegate;
	}

	@Override
	public T read(DataInput in) throws IOException, DataFormatException
	{
		return delegate.read(in);
	}

	@Override
	public void write(DataOutput out, T object) throws IOException
	{
		delegate.write(out, object);
	}

	public T readBytes(byte[] bytes) throws DataFormatException
	{
		if (null == bytes)
		{
			throw new IllegalArgumentException("bytes should not be null");
		}

		T object = null;
		DataInputStream in = new DataInputStream(
				new ByteArrayInputStream(bytes));
		try
		{
			object = read(in);
			in.close();
		}
		catch (DataFormatException ex)
		{
			throw ex;
		}
		catch (EOFException ex)
		{
			throw new DataFormatException("byte array not long enough", ex);
		}
		catch (IOException ex)
		{
			throw new DataFormatException("data corrupted", ex);
		}

		return object;
	}

	public byte[] writeBytes(T object)
	{
		if (null == object)
		{
			throw new IllegalArgumentException("object should not be null");
		}

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try
		{
			DataOutputStream out = new DataOutputStream(bytes);
			write(out, object);
			out.close();
		}
		catch (IOException ex)
		{
			ex.printStackTrace();
			assert (false);
		}

		return bytes.toByteArray();
	}
}
