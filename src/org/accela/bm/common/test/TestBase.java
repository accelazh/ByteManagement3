package org.accela.bm.common.test;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.accela.bm.common.BytePersistanceDelegate;
import org.accela.bm.common.PersistanceDelegate;

import junit.framework.TestCase;

public abstract class TestBase extends TestCase
{
	protected final File testFile = new File("testFile.txt");;

	protected final File dataFile = new File("dataFile.txt");

	protected final Random rand = new Random();

	protected final BytePersistanceDelegate<String> delegate = new BytePersistanceDelegate<String>(
			new StringPersistanceDelegate());

	private static class StringPersistanceDelegate implements
			PersistanceDelegate<String>
	{
		@Override
		public String read(DataInput in) throws IOException
		{
			return in.readUTF();
		}

		@Override
		public void write(DataOutput out, String object) throws IOException
		{
			out.writeUTF(object);
		}
	}

	@Override
	protected void setUp() throws Exception
	{
		close();

		testFile.delete();
		if (testFile.exists())
		{
			throw new IOException("can't remove testFile");
		}

		reopen((Object) null);
	}

	@Override
	protected void tearDown() throws Exception
	{
		close();
	}

	protected abstract void close() throws IOException;

	protected abstract void open(Object... args) throws IOException;

	protected void reopen(Object... args) throws IOException
	{
		close();
		open(args);
	}

	protected String readText(File file) throws IOException
	{
		StringBuffer buf = new StringBuffer();
		BufferedReader in = new BufferedReader(new FileReader(file));
		String line = null;
		while ((line = in.readLine()) != null)
		{
			buf.append(line);
			buf.append("\n");
		}

		return buf.toString();
	}

	protected byte[] genBytes(int length)
	{
		byte[] bs = new byte[length];
		rand.nextBytes(bs);
		return bs;
	}

	protected byte[] genBytesRnd(int length)
	{
		return genBytes(rand.nextInt(length + 1));
	}

	protected String genStr(int length)
	{
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < length; i++)
		{
			buf.append((char) rand.nextInt());
		}

		String str = buf.toString();
		assert (str.length() == length);

		return str;
	}

	protected String genStrRnd(int length)
	{
		return genStr(rand.nextInt(length + 1));
	}

	protected void fillFileRandom(long length) throws IOException
	{
		RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
		byte[] buf = new byte[(int) Math.min(length, 1024 * 1024 * 16)];
		rand.nextBytes(buf);
		long pos = 0;
		while (pos < length)
		{
			int step = (int) Math.min(buf.length, length - pos);
			raf.write(buf, 0, step);
			pos += step;
		}
		assert (length == pos);
		raf.close();
	}
}
