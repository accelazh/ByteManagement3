package org.accela.bm.pool.impl.binaryPool;

public class PoolSize
{
	private boolean zero = true;

	private byte exp = Common.MIN_LEN_EXP;

	public PoolSize()
	{
		this.zero = true;
		this.exp = Common.MIN_LEN_EXP;
	}

	private boolean checkValid()
	{
		if (zero)
		{
			return true;
		}

		return Common.checkLenExp(exp);
	}

	public synchronized boolean isZero()
	{
		return this.zero;
	}

	public synchronized byte getExp()
	{
		assert (checkValid());
		if (zero)
		{
			throw new IllegalStateException("poolSize == zero, no exp");
		}

		return exp;
	}

	public synchronized long getValue()
	{
		assert (checkValid());
		if (zero)
		{
			return 0;
		}

		return Common.expToValue(exp);
	}

	public synchronized void setZero()
	{
		assert (checkValid());
		this.zero = true;
	}

	public synchronized void setExp(byte exp)
	{
		assert (checkValid());
		if (!Common.checkLenExp(exp))
		{
			throw new IllegalArgumentException("exp illegal");
		}

		this.zero = false;
		this.exp = exp;
	}

	public synchronized void setValueTolerately(long value)
	{
		value = Math.min(Common.MAX_LONG_FROM_EXP, Math.max(0, value));

		if (0 == value)
		{
			setZero();
			return;
		}

		byte exp = Common.valueToExp(value);
		exp = (byte) Math.min(Common.MAX_LEN_EXP,
				Math.max(Common.MIN_LEN_EXP, exp));

		setExp(exp);
	}

	public synchronized void setDouble()
	{
		assert (checkValid());
		if (zero)
		{
			throw new IllegalStateException("poolSize == zero, no exp");
		}
		if (exp >= Common.MAX_LEN_EXP)
		{
			throw new IllegalStateException(
					"exp >= Common.MAX_LEN_EXP, too large");
		}

		exp++;
	}

	public synchronized void setHalf()
	{
		assert (checkValid());
		if (zero)
		{
			throw new IllegalStateException("poolSize == zero, no exp");
		}
		if (exp <= Common.MIN_LEN_EXP)
		{
			throw new IllegalStateException(
					"exp <= Common.MIN_LEN_EXP, too small");
		}

		exp--;
	}
}
