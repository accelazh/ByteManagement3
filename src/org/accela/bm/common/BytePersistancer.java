package org.accela.bm.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

public class BytePersistancer implements Persistancer
{
	private Persistancer persistancer = null;

	public BytePersistancer(Persistancer persistancer)
	{
		if (null == persistancer)
		{
			throw new IllegalArgumentException(
					"persistancer should not be null");
		}

		this.persistancer = persistancer;
	}

	public Persistancer getPersistancer()
	{
		return persistancer;
	}

	@Override
	public void create(Object... args) throws IOException
	{
		this.persistancer.create(args);
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		this.persistancer.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		this.persistancer.write(out);
	}

	public void readBytes(byte[] bytes) throws DataFormatException
	{
		if (null == bytes)
		{
			throw new IllegalArgumentException("bytes should not be null");
		}

		DataInputStream in = new DataInputStream(
				new ByteArrayInputStream(bytes));

		try
		{
			this.read(in);
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
	}

	public byte[] writeBytes()
	{
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try
		{
			DataOutputStream out = new DataOutputStream(bytes);
			write(out);
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
