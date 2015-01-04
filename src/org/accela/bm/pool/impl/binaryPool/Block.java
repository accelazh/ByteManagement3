package org.accela.bm.pool.impl.binaryPool;

import java.io.IOException;
import java.util.Arrays;

import org.accela.bm.pool.BlockIndexOutOfBoundsException;
import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.BytePool;

public class Block implements ByteAccesser
{
	private static final int HEADER_LEN = 2;

	private static final int IN_FREE_LIST_SIGN_LEN = 1;

	public static final int APPEND_LEN = HEADER_LEN + IN_FREE_LIST_SIGN_LEN;

	public static final int POINTER_LEN = Common.LONG_SIZE * 2;

	// =======================================================

	private long pos = BytePool.NULL; // Note：-1表示NULL地址

	private ByteAccesser accesser = null;

	private boolean free = false; // 在Pool完成alloc/free流程的最后才被更改

	private byte lenExp = Common.MIN_LEN_EXP;

	private static byte IN_FREE_LIST_TRUE = (byte) 0xAA; // 特殊的取值有防止数据出错，以及维持读写原子性的功能

	private static byte IN_FREE_LIST_FALSE = 0x55;

	private boolean inFreeList = false; // Block一旦被移入或者移出FreeList，立即被更改，肩负维护FreeList防重复取出/删除/放入Block的职责

	private long prev = BytePool.NULL;

	private long next = BytePool.NULL;

	public Block(long pos, ByteAccesser accesser)
	{
		if (pos < 0)
		{
			throw new IllegalArgumentException("pos should not be negative");
		}
		if (null == accesser)
		{
			throw new IllegalArgumentException("accesser should not be null");
		}

		this.pos = pos;
		this.accesser = accesser;
	}

	public long getPos()
	{
		return pos;
	}

	public ByteAccesser getAccesser()
	{
		return accesser;
	}

	public boolean isFree()
	{
		return free;
	}

	public void setFree(boolean free)
	{
		this.free = free;
	}

	private boolean checkLenExp(byte lenExp)
	{
		if (!Common.checkLenExp(lenExp))
		{
			return false;
		}
		if (!Common.checkAddrMatchLen(this.pos, lenExp))
		{
			// System.out.println("Block checkLenExp failure");
			return false;
		}

		return true;
	}

	// Note：不检查pointer与pos相等，不检查prev与next相等。因为这两种错误
	// 可能由于IO或者并发错误引起，不便于抛出异常，并且职责不再Block
	private boolean checkPointer(long pointer)
	{
		if (pointer < 0)
		{
			if (pointer != BytePool.NULL)
			{
				// System.out.println("Block checkPointer failure");
				return false;
			}
		}
		else
		{
			if (!Common.checkAddrMatchLen(pos, lenExp))
			{
				// System.out.println("Block checkPointer failure");
				return false;
			}
		}

		return true;
	}

	public byte getLenExp()
	{
		assert (Common.checkLenExp(lenExp));
		return lenExp;
	}

	public void setLenExp(byte lenExp)
	{
		if (!checkLenExp(lenExp))
		{
			throw new IllegalArgumentException("lenExp illegal");
		}

		this.lenExp = lenExp;
	}

	public long dataLength()
	{
		return length() - APPEND_LEN;
	}

	@Override
	public long length()
	{
		return Common.expToValue(getLenExp());
	}

	public boolean isInFreeList()
	{
		return inFreeList;
	}

	public void setInFreeList(boolean inFreeList)
	{
		this.inFreeList = inFreeList;
	}

	public long getPrev()
	{
		assert (checkPointer(prev));
		return prev;
	}

	public void setPrev(long prev)
	{
		if (!checkPointer(prev))
		{
			throw new IllegalArgumentException("prev does not match lenExp");
		}

		this.prev = prev;
	}

	public long getNext()
	{
		assert (checkPointer(next));
		return next;
	}

	public void setNext(long next)
	{
		if (!checkPointer(next))
		{
			throw new IllegalArgumentException("next does not match lenExp");
		}

		this.next = next;
	}

	// ===============================================================

	public boolean loadHeader() throws IOException
	{
		byte[] data = new byte[HEADER_LEN];
		accesser.get(data, 0, pos, data.length);

		byte head = data[0];
		byte checkSum = data[1];
		if (checkSum != org.accela.bm.common.Common.checkSum(head))
		{
			// System.out.println("Block loadHeader failure: check sum invalid");
			return false;
		}

		this.free = (head & 0x80) != 0;
		this.lenExp = (byte) (head & 0x7F);
		if (!checkLenExp(lenExp))
		{
			// System.out
			// .println("Block loadHeader failure: check lenExp invalid");
			return false;
		}

		return true;
	}

	public void saveHeader() throws IOException
	{
		byte head = lenExp;
		if (free)
		{
			head |= 0x80;
		}
		else
		{
			head &= 0x7F;
		}
		byte checkSum = org.accela.bm.common.Common.checkSum(head);

		byte[] data = new byte[] { head, checkSum };
		assert (data.length == HEADER_LEN);
		accesser.set(data, 0, pos, data.length);
	}

	public void clearHeader() throws IOException
	{
		byte[] data = new byte[HEADER_LEN];
		Arrays.fill(data, (byte) 0);
		accesser.set(data, 0, pos, data.length);
	}

	public boolean loadInFreeListSign() throws IOException
	{
		byte[] data = new byte[IN_FREE_LIST_SIGN_LEN];
		accesser.get(data, 0, pos + HEADER_LEN, data.length);
		if (data[0] == IN_FREE_LIST_TRUE)
		{
			this.inFreeList = true;
			return true;
		}
		else if (data[0] == IN_FREE_LIST_FALSE)
		{
			this.inFreeList = false;
			return true;
		}
		else
		{
			this.inFreeList = false;
			// System.out.println("Block loadInFreeListSign Failure");
			return false;
		}
	}

	public void saveInFreeListSign() throws IOException
	{
		byte[] data = new byte[IN_FREE_LIST_SIGN_LEN];
		if (this.inFreeList)
		{
			data[0] = IN_FREE_LIST_TRUE;
		}
		else
		{
			data[0] = IN_FREE_LIST_FALSE;
		}

		assert (data.length == IN_FREE_LIST_SIGN_LEN);
		accesser.set(data, 0, pos + HEADER_LEN, data.length);
	}

	public void clearInFreeListSign() throws IOException
	{
		byte[] data = new byte[IN_FREE_LIST_SIGN_LEN];
		Arrays.fill(data, (byte) 0);
		accesser.set(data, 0, pos + HEADER_LEN, data.length);
	}

	public void loadPointer() throws IOException
	{
		byte[] data = new byte[POINTER_LEN];
		accesser.get(data, 0, pos + APPEND_LEN, data.length);

		long prevPointer = Common.byteToLong(data, 0);
		long nextPointer = Common.byteToLong(data, Common.LONG_SIZE);

		if (!checkPointer(prevPointer))
		{
			// System.out.println("Block loadPointer failure");
			prevPointer = BytePool.NULL;
		}
		if (!checkPointer(nextPointer))
		{
			// System.out.println("Block loadPointer failure");
			nextPointer = BytePool.NULL;
		}

		this.prev = prevPointer;
		this.next = nextPointer;
	}

	public void savePointer() throws IOException
	{
		assert (checkPointer(this.prev));
		assert (checkPointer(this.next));

		byte[] data = new byte[POINTER_LEN];
		Common.longToByte(data, 0, this.prev);
		Common.longToByte(data, Common.LONG_SIZE, this.next);

		accesser.set(data, 0, pos + APPEND_LEN, data.length);
	}

	// ================================================================

	private void testBounds(long idx, int length)
	{
		if (idx < 0)
		{
			throw new IllegalArgumentException("idx < 0");
		}
		if (length < 0)
		{
			throw new IllegalArgumentException("length < 0");
		}
		if (idx + length > dataLength())
		{
			throw new BlockIndexOutOfBoundsException("idx + length > dataLen");
		}

		return;
	}

	private long localIdxToGlobal(long idx)
	{
		return idx + pos + APPEND_LEN;
	}

	@Override
	public void get(byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		testBounds(idx, length);
		accesser.get(buf, bufOffset, localIdxToGlobal(idx), length);
	}

	@Override
	public void set(byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		testBounds(idx, length);
		accesser.set(buf, bufOffset, localIdxToGlobal(idx), length);
	}

	@Override
	public void setLength(long newLength) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void flush() throws IOException
	{
		// do nothing
	}

	@Override
	public void close() throws IOException
	{
		// do nothing
	}

	// ======================================================================

	private boolean isUpBuddy()
	{
		return (this.pos & this.length()) == 0;
	}

	// Note：这里没有检查isFree()
	public boolean isBuddy(Block block)
	{
		if (null == block)
		{
			throw new IllegalArgumentException("block should not be null");
		}

		if (block.getLenExp() != this.getLenExp())
		{
			return false;
		}
		if (block.getPos() == this.getPos())
		{
			return false;
		}
		if ((block.getPos() >> (block.getLenExp() + 1)) != (this.getPos() >> (this
				.getLenExp() + 1)))
		{
			return false;
		}

		assert (this.isUpBuddy() != block.isUpBuddy());
		return true;

	}

	public Block getBuddy(PoolSize poolSize) throws IOException
	{
		Block buddy = Common.tryGetBlock(this.getPos()
				+ (isUpBuddy() ? 1 : (-1))
				* this.length(), this.getLenExp(), poolSize, accesser);
		if (null == buddy || !isBuddy(buddy))
		{
			return null;
		}
		return buddy;
	}

	public Block[] divide() throws IOException
	{
		if (this.getLenExp() <= Common.MIN_LEN_EXP)
		{
			throw new IllegalStateException("can't divide, too small");
		}

		Block upBuddy = new Block(this.getPos(), this.getAccesser());
		upBuddy.setFree(this.isFree());
		upBuddy.setLenExp((byte) (this.getLenExp() - 1));

		Block downBuddy = new Block(this.getPos() + this.length() / 2,
				this.getAccesser());
		downBuddy.setFree(this.isFree());
		downBuddy.setLenExp((byte) (this.getLenExp() - 1));

		downBuddy.setInFreeList(false);
		downBuddy.saveInFreeListSign();

		return new Block[] { upBuddy, downBuddy };
	}

	public Block merge(Block buddy) throws IOException
	{
		if (null == buddy)
		{
			throw new IllegalArgumentException("buddy should not be null");
		}
		if (buddy.isFree() != this.isFree())
		{
			throw new IllegalArgumentException("isFree() not equals");
		}
		if (!isBuddy(buddy))
		{
			throw new IllegalArgumentException("not buddy");
		}
		if (this.getLenExp() >= Common.MAX_LEN_EXP)
		{
			throw new IllegalStateException("can't merge, too largeg");
		}

		Block upBuddy = null;
		Block downBuddy = null;
		if (this.isUpBuddy())
		{
			upBuddy = this;
			downBuddy = buddy;
		}
		else
		{
			upBuddy = buddy;
			downBuddy = this;
		}

		Block big = new Block(upBuddy.getPos(), upBuddy.getAccesser());
		big.setFree(upBuddy.isFree());
		big.setLenExp((byte) (upBuddy.getLenExp() + 1));

		downBuddy.clearHeader();
		downBuddy.clearInFreeListSign();

		return big;
	}

	@Override
	public String toString()
	{
		return "Block [accesser="
				+ accesser
				+ ", free="
				+ free
				+ ", inFreeList="
				+ inFreeList
				+ ", lenExp="
				+ lenExp
				+ ", next="
				+ next
				+ ", pos="
				+ pos
				+ ", prev="
				+ prev
				+ "]";
	}

}
