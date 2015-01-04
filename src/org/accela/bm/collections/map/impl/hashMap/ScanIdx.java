package org.accela.bm.collections.map.impl.hashMap;

public class ScanIdx
{
	private long value = 0;

	private long from = 0;

	private long to = 0;

	public ScanIdx(long from, long to)
	{
		this(from, from, to);
	}

	public ScanIdx(long value, long from, long to)
	{
		if (from > to)
		{
			throw new IllegalArgumentException("from > to");
		}
		if (value < from || value > to)
		{
			throw new IllegalArgumentException("illegal value");
		}

		this.value = value;
		this.from = from;
		this.to = to;
	}

	public long getFrom()
	{
		return from;
	}

	public long getTo()
	{
		return to;
	}

	public boolean isFinished(long idx)
	{
		return idx >= to;
	}

	public boolean isFinished()
	{
		return value >= to;
	}

	public synchronized long get()
	{
		assert (this.value >= from && this.value <= to);
		return this.value;
	}

	public synchronized void set(long value)
	{
		if (value < from || value > to)
		{
			throw new IllegalArgumentException("illegal value");
		}

		this.value = value;
	}

	public synchronized long getAndIncr()
	{
		if (value >= to)
		{
			assert (value == to);
			return to;
		}

		return value++;
	}

}
