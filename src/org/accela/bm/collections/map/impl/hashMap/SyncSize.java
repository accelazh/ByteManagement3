package org.accela.bm.collections.map.impl.hashMap;

public class SyncSize
{
	private long value = 0;

	public SyncSize(long value)
	{
		if (value < 0)
		{
			throw new IllegalArgumentException("value < 0");
		}

		this.value = value;
	}

	public synchronized long get()
	{
		assert (this.value >= 0);
		return this.value;
	}

	public synchronized void set(long value)
	{
		if (value < 0)
		{
			throw new IllegalArgumentException("value < 0");
		}
		this.value = value;
	}

	public synchronized void incr()
	{
		this.value++;
	}

	public synchronized void decr()
	{
		this.value--;
	}

	public synchronized void clear()
	{
		this.value = 0;
	}
}
