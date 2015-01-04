package org.accela.bm.pool.impl.binaryPool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.accela.bm.common.DataFormatException;
import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.BytePool;
import org.accela.bm.pool.impl.AbstractBytePool;

/*
 * 设计思路：
 * 1.Block的长度为按照2的幂。这种特殊的Block长度机制，使得一系列特殊的技巧能够使用
 * （Block的长度仅用1字节表示，加上校验码仅需2字节，大大节省空间；Block的长度和地址
 * 的匹配测试能够进行；Block的分割与合并简单，并且能够支持从32字节到2^60以上的大小，
 * 等等）。
 * 2.Block的分配，能够分割大Block。Block释放时，会立即与可能的Block合并成更大的Block。
 * 3.池的大小能够自动缩减。策略是，当池的已使用空间为池总大小的1/4时，池的大小缩减成
 * 原来的1/2。这个策略考虑了池大小扩张与缩减的抗抖动。
 * 
 * Features: 
 * 1. Block的冗余校验机制，使得能够识别坏块。
 * 2. 如果使用错误的key来访问Block，冗余校验机制以及Block长度和地址的匹配测试
 * 	     能够在多数情况下发现错误
 * 3. Block访问越界检查
 * 3. FreeList使用与加载链表头的技术，减少磁盘访问。
 * 4. alloc、free流程，磁盘访问被优化，Block的header信息尽量仅在流程的开头读、结尾写，
 * 减少IO次数。
 * 5. 高度的可靠性。Block损坏、自由链表损坏，仅仅造成损坏或者遗失的磁盘空间无法使用，
 * 池仍然能够继续工作。即使用随机数据抹掉池所用的文件，池仍然能够继续工作，只是被抹掉的
 * 数据会丢失，被抹掉的空间可能无法利用。
 * 6. 并发控制被优化。alloc、free、get/set、expand/shrink，均可以并行工作；各个自由链表
 * 均可以并行工作。但是有一些限制，后面会讲。
 * 7. 参见Block.getBuddy()，对于一个block，能够与它配对merge的block有且仅有一块。这构成
 * 了二分法merge/divide的关键。对多线程模型也是其基础之一。
 * 
 * 
 * 并发模型：
 * 0. 参见Features.7。
 * 1. 每条FreeList自己管理自己的同步。FreeList组管理空闲Block的并发控制。
 * 2. alloc、free在使用空闲Block时，一定通过FreeList。FreeList保证同一个空闲Block不会被
 * 多个线程持有。（FreeList的remove方法、retrieve方法，保证同一个Block不能够被remove或者
 * retrieve两次）
 * 3. Block的header的读和写，因为其特殊的存储和使用方式，保证Block的header如果仅
 * 写了一半，读取出来会被判为broken（Block损坏），从而无法使用。简单说就是，近似的
 * 原子性。第二，alloc、free流程只有在最后才 将修改的Block的header信息保存到磁盘，
 * 从而达到这个效果：alloc、free等方法在操纵一个Block且未完成时，该Block的header无法
 * 被另一个线程的alloc、free方法所使用。
 * 4. 池的大小扩张和缩减的过程，需要单独同步。这些过程修改poolSize，小心设计poolSize
 * 被修改的顺序，能够控制alloc、free等可见的池区域为不受池大小变化影响的区域，从而
 * 实现池大小变化与alloc、free等流程的并发。
 * 5. 由于对于已分配的Block没有并发控制办法，因此并发模型有一定限制，见下文
 * 6. 靠上面的描述，如果还是难以理解并发控制，并不奇怪，我想了半个月呢。现在的方案
 * 是在许多方案的对比和演化中诞生的，中心思想是最大化并发。许多信息，以及我的思考过程
 * 都没有被写下来，要看懂可能需要先自己思考并设计出一套并发方案，然后读代码和我的对比，
 * 这样才能理解许多没有写下来的设计。
 * 
 * =========================================================================
 * 并发模型的限制：
 * 1. 不能在一个Block的free操作未完成时，再次free这个Block。
 * 2. 不能在一个Block的free操作未完成时，get/set这个Block。
 * =========================================================================
 * 
 * 缺陷:
 * 1. Block大小按照2的指数幂排列，分配时相当浪费空间，但是占用空间最多是有效空间的两倍
 * 2. Block一被释放就尝试merge，分配时Block先尝试divide；merge和divide可能会形成抖动
 * 3. 多线程下，为保证并发性，Block的merge操作可能不充分，从而池大小的shrink可能不充分。
 * 最简单情况是，如果Block A merge时需要B，Block B merge时需要A，A、B在相近时间内均被
 * free，则可能导致：A检测B是否可以用来merge时，B的free流程正进行到一半，检测失败；B对A
 * 同理，因而A、B均不能merge。而当A、B均free流程结束后，却本应该merge的。
 */
public class BinaryBytePool extends AbstractBytePool
{
	private FreeList[] freeLists = new FreeList[Common.MAX_LEN_EXP
			- Common.MIN_LEN_EXP
			+ 1];

	private PoolSize poolSize = null;

	private ByteAccesser accesser = null;

	private Lock poolExpandShrinkLock = new ReentrantLock();

	public BinaryBytePool(ByteAccesser accesser)
	{
		if (null == accesser)
		{
			throw new IllegalArgumentException("accesser should not be null");
		}

		this.accesser = accesser;
	}

	@Override
	public ByteAccesser getAccesser()
	{
		return this.accesser;
	}

	private void init(long poolSize, long[] freeListHeadPoses)
			throws IOException
	{
		// init poolSize
		this.poolSize = new PoolSize();
		this.poolSize.setValueTolerately(poolSize);
		this.accesser.setLength(this.poolSize.getValue());

		// init free list
		if (null == freeListHeadPoses)
		{
			freeListHeadPoses = new long[0];
		}
		long[] wildFreeListHeadPoses = freeListHeadPoses;
		for (int i = 0; i < wildFreeListHeadPoses.length; i++)
		{
			wildFreeListHeadPoses[i] = Math.max(BytePool.NULL,
					wildFreeListHeadPoses[i]);
		}
		freeListHeadPoses = new long[freeLists.length];
		Arrays.fill(freeListHeadPoses, BytePool.NULL);
		System.arraycopy(wildFreeListHeadPoses, 0, freeListHeadPoses, 0, Math
				.min(wildFreeListHeadPoses.length, freeListHeadPoses.length));

		for (int i = 0; i < freeListHeadPoses.length; i++)
		{
			freeLists[i] = new FreeList(freeListHeadPoses[i],
					(byte) (i + Common.MIN_LEN_EXP), accesser);
		}
	}

	@Override
	public void create(Object... args) throws IOException
	{
		init(0, null);
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		long poolSize = in.readLong();
		long[] freeListHeadPoses = new long[freeLists.length];
		for (int i = 0; i < freeListHeadPoses.length; i++)
		{
			freeListHeadPoses[i] = in.readLong();
		}

		try
		{
			init(poolSize, freeListHeadPoses);
		}
		catch (RuntimeException ex)
		{
			assert (false);
			throw new DataFormatException(ex);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(poolSize.getValue());
		for (int i = 0; i < freeLists.length; i++)
		{
			out.writeLong(freeLists[i].getHeadPos());
		}
	}

	@Override
	public void flush() throws IOException
	{
		accesser.flush();
	}

	@Override
	public void close() throws IOException
	{
		accesser.close();
	}

	@Override
	public long poolSize()
	{
		assert (checkPoolSizeValid());
		return poolSize.getValue();
	}

	private boolean checkPoolSizeValid()
	{
		poolExpandShrinkLock.lock();
		try
		{
			try
			{
				return accesser.length() == poolSize.getValue();
			}
			catch (IOException ex)
			{
				ex.printStackTrace();
				return false;
			}
		}
		finally
		{
			poolExpandShrinkLock.unlock();
		}
	}

	// =======================================================================

	private byte roundSizeToExp(long size)
	{
		byte exp = Common.valueToExp(size + Block.APPEND_LEN);
		exp = (byte) Math.max(exp, Common.MIN_LEN_EXP);
		if (exp > Common.MAX_LEN_EXP)
		{
			throw new IllegalArgumentException("too large to alloc: " + size);
		}

		assert (Common.checkLenExp(exp));
		return exp;
	}

	private int selectFreeList(byte lenExp)
	{
		assert (Common.checkLenExp(lenExp));
		int idx = lenExp - Common.MIN_LEN_EXP;
		assert (freeLists[idx].getLenExp() == lenExp);

		return idx;
	}

	private Block retrieveFromFreeList(int idx) throws IOException
	{
		return freeLists[idx].retrieve(poolSize);
	}

	private Block findCapableFromFreeList(byte lenExp) throws IOException
	{
		for (int i = selectFreeList(lenExp); i < freeLists.length; i++)
		{
			Block block = retrieveFromFreeList(i);
			if (block != null)
			{
				assert (block.getLenExp() >= lenExp);
				return block;
			}
		}
		return null;
	}

	// NOTE: 所有返回Block的方法，Block一定未存储；所有Block传入并不返回的方法
	// 一定假设Block传入时已经加载过
	private boolean putToFreeList(Block block) throws IOException
	{
		return freeLists[selectFreeList(block.getLenExp())]
				.put(block, poolSize);
	}

	private boolean removeFromFreeList(Block block) throws IOException
	{
		return freeLists[selectFreeList(block.getLenExp())].remove(block,
				poolSize);
	}

	// 访问一个Block，但在进行同步控制之前不能修改它
	private Block peekBlock(long pos) throws IOException
	{
		return Common.tryGetBlock(pos, poolSize, accesser);
	}

	// 访问一个Block，但在进行同步控制之前不能修改它
	private Block peekBlock(long pos, byte lenExp) throws IOException
	{
		return Common.tryGetBlock(pos, lenExp, poolSize, accesser);
	}

	private Block tryGetAllocatedBlock(long pos) throws IOException
	{
		Block block = Common.tryGetBlock(pos, poolSize, accesser);
		if (null == block)
		{
			return null;
		}
		if (block.isFree())
		{
			return null;
		}

		return block;
	}

	private Block mergeBlock(Block block) throws IOException
	{
		assert (block.isFree());
		if (block.getLenExp() >= Common.MAX_LEN_EXP)
		{
			return block;
		}
		if (poolSize.isZero() || block.getLenExp() >= poolSize.getExp())
		{
			return block;
		}

		Block buddy = block.getBuddy(poolSize);
		if (null == buddy)
		{
			return block;
		}
		// System.out.println("merge block get buddy(not removed): buddy=" +
		// buddy);

		if (!buddy.isFree())
		{
			return block;
		}
		// 这一步的成功保证merge操作独占buddy，从而避免与多线程同时使用一个Block
		boolean succRemoved = removeFromFreeList(buddy);
		if (!succRemoved)
		{
			return block;
		}

		// System.out.println(Thread.currentThread()
		// + "merge block: block="
		// + block
		// + ", buddy="
		// + buddy);
		return block.merge(buddy);
	}

	private Block mergeBlockAsCan(Block block) throws IOException
	{
		Block cur = block;
		Block big = cur;
		while ((big = mergeBlock(cur)) != cur)
		{
			assert (big.getLenExp() == cur.getLenExp() + 1) : big.getLenExp()
					+ ", "
					+ cur.getLenExp();
			cur = big;
		}

		assert (cur.getPos() + cur.length() <= poolSize());
		return cur;
	}

	private Block[] divideBlock(Block block) throws IOException
	{
		assert (block.getLenExp() > Common.MIN_LEN_EXP);
		assert (block.isFree());

		// System.out.println("divide block: block=" + block);
		return block.divide();
	}

	private Block divideBlockToLen(Block block, byte lenExp) throws IOException
	{
		assert (Common.checkLenExp(lenExp));
		assert (block.getLenExp() >= lenExp);
		assert (block.isFree());

		Block cur = block;
		while (cur.getLenExp() > lenExp)
		{
			Block[] pair = divideBlock(cur);
			putToFreeList(pair[1]);
			cur = pair[0];
		}

		assert (cur.getLenExp() == lenExp);
		return cur;
	}

	// NOTE: 当Block的大小和poolSize一样大的时候，merge操作会EOFException
	private Block firstExpand(byte lenExp) throws IOException
	{
		// System.out.println("firstExpand: lenExp=" + lenExp);

		assert (Common.checkLenExp(lenExp));
		assert (poolSize.isZero());

		accesser.setLength(Common.expToValue(lenExp));

		Block block = new Block(0, accesser);
		block.setLenExp(lenExp);

		block.setFree(true);
		block.saveHeader(); // 防止Block被误free
		block.setInFreeList(false);
		block.saveInFreeListSign(); // 防止Block被误FreeList.remove

		// 同步控制:只有当expand完成时，才改变poolSize的值
		poolSize.setExp(lenExp);
		assert (accesser.length() == poolSize.getValue());
		return block;
	}

	private Block expand() throws IOException
	{
		// System.out.println("expand: oldPoolSizeExp=" + poolSize.getExp());

		assert (!poolSize.isZero());
		if (poolSize.getExp() >= Common.MAX_LEN_EXP)
		{
			return null;
		}

		assert (accesser.length() == poolSize.getValue());

		accesser.setLength(poolSize.getValue() * 2);

		Block block = new Block(poolSize.getValue(), accesser);
		block.setLenExp(poolSize.getExp());

		block.setFree(true);
		block.saveHeader(); // 防止Block被误free
		block.setInFreeList(false);
		block.saveInFreeListSign(); // 防止Block被误FreeList.remove

		// 同步控制:只有当expand完成时，才改变poolSize的值
		poolSize.setDouble();
		assert (accesser.length() == poolSize.getValue());
		return block;
	}

	// NOTE: 空间满是返回null；返回的Block的大小可能大于lenExp
	private Block expandAllocCapable(byte lenExp) throws IOException
	{
		assert (Common.checkLenExp(lenExp));
		if (poolSize.isZero())
		{
			return firstExpand(lenExp);
		}

		Block cur = null;
		while (true)
		{
			cur = expand();
			if (null == cur)
			{
				return null;
			}
			else if (cur.getLenExp() < lenExp)
			{
				this.putToFreeList(cur);
				continue;
			}
			else
			{
				return cur;
			}
		}
	}

	private Block freeListAllocCapable(byte lenExp) throws IOException
	{
		assert (Common.checkLenExp(lenExp));
		Block block = this.findCapableFromFreeList(lenExp);
		assert (null == block || (block.isFree() && !block.isInFreeList()));
		return block;
	}

	private Block allocBlock(byte lenExp) throws IOException
	{
		assert (Common.checkLenExp(lenExp));

		Block block = freeListAllocCapable(lenExp);
		if (null == block)
		{
			poolExpandShrinkLock.lock();
			try
			{
				block = freeListAllocCapable(lenExp);
				if (null == block)
				{
					block = expandAllocCapable(lenExp);
				}
			}
			finally
			{
				poolExpandShrinkLock.unlock();
			}
		}
		if (null == block)
		{
			return null;
		}

		block = divideBlockToLen(block, lenExp);
		assert (block.getLenExp() == lenExp);
		assert (block.isFree());
		return block;
	}

	private void shrinkWhole() throws IOException
	{
		// System.out.println("shrink by whole");

		assert (!poolSize.isZero());
		assert (accesser.length() == poolSize.getValue());

		poolSize.setZero();
		accesser.setLength(0);

		assert (accesser.length() == poolSize.getValue());
	}

	private void shrinkHalf() throws IOException
	{
		// System.out.println("shrink by half");

		assert (!poolSize.isZero());
		assert (poolSize.getExp() >= Common.MIN_LEN_EXP + 1);
		assert (accesser.length() == poolSize.getValue());

		poolSize.setHalf();
		accesser.setLength(poolSize.getValue());

		assert (accesser.length() == poolSize.getValue());
	}

	private boolean shrinkByBlockLargeAsPool(Block block) throws IOException
	{
		assert (!poolSize.isZero());
		assert (block.getPos() == 0);
		assert (block.getLenExp() == poolSize.getExp());

		shrinkWhole();
		return true;
	}

	// 池的下半段将被检测shrink
	private boolean shrinkByBlockToTruncate(Block block) throws IOException
	{
		assert (!poolSize.isZero());
		assert (poolSize.getExp() >= Common.MIN_LEN_EXP + 2);
		assert (block.getPos() == poolSize.getValue() / 2);
		assert (block.getLenExp() == poolSize.getExp() - 1);

		Block test = peekBlock(0);
		if (test != null
				&& test.getLenExp() >= poolSize.getExp() - 2
				&& test.isFree())
		{
			shrinkHalf();
			return true;
		}

		test = peekBlock(poolSize.getValue() / 4,
				(byte) (poolSize.getExp() - 2));
		if (test != null && test.isFree())
		{
			shrinkHalf();
			return true;
		}

		return false;
	}

	// 池的上半段或者上1/4段或者上半段的下半段将被检测shrink
	private boolean shrinkByBlockForSpace(Block block) throws IOException
	{
		assert (!poolSize.isZero());
		assert (poolSize.getExp() >= Common.MIN_LEN_EXP + 2);
		assert (block.getLenExp() >= poolSize.getExp() - 2);
		assert (block.getPos() == 0 || block.getPos() == poolSize.getValue() / 4);
		assert (block.getLenExp() < poolSize.getExp());

		Block test = peekBlock(poolSize.getValue() / 2,
				(byte) (poolSize.getExp() - 1));
		if (null == test || !test.isFree())
		{
			return false;
		}
		// 这一步的成功保证shrink操作独占test，从而避免与多线程同时使用一个Block
		boolean succRemoved = removeFromFreeList(test);
		if (!succRemoved)
		{
			return false;
		}

		shrinkHalf();
		return true;
	}

	// Note: 传入参数block应该还没有被放入自由链表
	// 返回值为{是否shrink了，传入block是否被shrink掉了}
	private boolean[] tryShrink(Block block) throws IOException
	{
		boolean[] ret = new boolean[2];

		if (!poolSize.isZero()
				&& block.getPos() == 0
				&& block.getLenExp() == poolSize.getExp())
		{
			poolExpandShrinkLock.lock();
			try
			{
				//use double-checked lock pattern to enhance performance
				if (!poolSize.isZero()
						&& block.getPos() == 0
						&& block.getLenExp() == poolSize.getExp())
				{
					assert (block.getLenExp() == poolSize.getExp());
					assert (block.getPos() == 0);

					ret[0] = shrinkByBlockLargeAsPool(block);
					ret[1] = ret[0];

					return ret;
				}
			}
			finally
			{
				poolExpandShrinkLock.unlock();
			}

			// System.out.println("BinaryBytePool: recurisive tryShrink");
			return tryShrink(block);
		}
		if (!poolSize.isZero()
				&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
				&& block.getPos() == poolSize.getValue() / 2
				&& block.getLenExp() == poolSize.getExp() - 1)
		{
			poolExpandShrinkLock.lock();
			try
			{
				if (!poolSize.isZero()
						&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
						&& block.getPos() == poolSize.getValue() / 2
						&& block.getLenExp() == poolSize.getExp() - 1)
				{
					ret[0] = shrinkByBlockToTruncate(block);
					ret[1] = ret[0];

					return ret;
				}
			}
			finally
			{
				poolExpandShrinkLock.unlock();
			}

			// System.out.println("BinaryBytePool: recurisive tryShrink");
			return tryShrink(block);
		}
		if (!poolSize.isZero()
				&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
				&& block.getPos() == 0
				&& block.getLenExp() == poolSize.getExp() - 1)
		{
			poolExpandShrinkLock.lock();
			try
			{
				if (!poolSize.isZero()
						&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
						&& block.getPos() == 0
						&& block.getLenExp() == poolSize.getExp() - 1)
				{
					ret[0] = shrinkByBlockForSpace(block);
					ret[1] = false;

					return ret;
				}
			}
			finally
			{
				poolExpandShrinkLock.unlock();
			}

			// System.out.println("BinaryBytePool: recurisive tryShrink");
			return tryShrink(block);
		}
		if (!poolSize.isZero()
				&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
				&& block.getPos() == 0
				&& block.getLenExp() == poolSize.getExp() - 2)
		{
			poolExpandShrinkLock.lock();
			try
			{
				if (!poolSize.isZero()
						&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
						&& block.getPos() == 0
						&& block.getLenExp() == poolSize.getExp() - 2)
				{
					ret[0] = shrinkByBlockForSpace(block);
					ret[1] = false;

					return ret;
				}
			}
			finally
			{
				poolExpandShrinkLock.unlock();
			}

			// System.out.println("BinaryBytePool: recurisive tryShrink");
			return tryShrink(block);
		}
		if (!poolSize.isZero()
				&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
				&& block.getPos() == poolSize.getValue() / 4
				&& block.getLenExp() == poolSize.getExp() - 2)
		{
			poolExpandShrinkLock.lock();
			try
			{
				if (!poolSize.isZero()
						&& poolSize.getExp() >= Common.MIN_LEN_EXP + 2
						&& block.getPos() == poolSize.getValue() / 4
						&& block.getLenExp() == poolSize.getExp() - 2)
				{
					ret[0] = shrinkByBlockForSpace(block);
					ret[1] = false;

					return ret;
				}
			}
			finally
			{
				poolExpandShrinkLock.unlock();
			}

			// System.out.println("BinaryBytePool: recurisive tryShrink");
			return tryShrink(block);
		}

		ret[0] = false;
		ret[1] = false;

		return ret;
	}

	private boolean freeBlock(Block block) throws IOException
	{
		if (poolSize.isZero() || block.getLenExp() > poolSize.getExp())
		{
			return false;
		}
		if (block.isFree())
		{
			return false;
		}

		block.setFree(true);
		block = mergeBlockAsCan(block);
		boolean[] shrinkRet = tryShrink(block);
		if (!shrinkRet[1])
		{
			putToFreeList(block);
		}

		return true;
	}

	// NOTE: 这个方法虽然返回Block，但在返回前已经保存了Block的信息
	private Block doAlloc(long size) throws IOException
	{
		if (size < 0)
		{
			throw new IllegalArgumentException("size < 0");
		}
		Block block = allocBlock(roundSizeToExp(size));
		if (null == block)
		{
			throw new IOException("maxmium pool size reached: length="
					+ poolSize.getValue()
					+ ", lenExp="
					+ poolSize.getExp());
		}

		block.setFree(false);
		block.saveHeader();
		// System.out.println(Thread.currentThread()
		// + "pool.doAlloc() finished and save header: block="
		// + block);
		return block;
	}

	@Override
	public long alloc(long size) throws IOException
	{
		return doAlloc(size).getPos();
	}

	@Override
	public boolean free(long key) throws IOException
	{
		Block block = tryGetAllocatedBlock(key);
		if (null == block)
		{
			return false;
		}

		// System.out.println("free start to free a block: block=" + block);
		return freeBlock(block);
	}

	private void blockCopy(Block src, Block dst) throws IOException
	{
		assert (src != null);
		assert (dst != null);
		if (src.getPos() == dst.getPos())
		{
			return;
		}

		final int BUF_SIZE = 8096;

		long srcLen = src.dataLength();
		long dstLen = dst.dataLength();

		byte[] buf = new byte[(int) Math
				.min(Math.min(BUF_SIZE, srcLen), dstLen)];

		long curIdx = 0;
		while ((curIdx + buf.length) <= srcLen
				&& (curIdx + buf.length) <= dstLen)
		{
			src.get(buf, 0, curIdx, buf.length);
			dst.set(buf, 0, curIdx, buf.length);
			curIdx += buf.length;
		}

		int remain = (int) Math.min(srcLen - curIdx, dstLen - curIdx);
		if (remain > 0)
		{
			src.get(buf, 0, curIdx, remain);
			dst.set(buf, 0, curIdx, remain);
		}
	}

	@Override
	protected long realloc(long key, long size, boolean copy)
			throws IOException
	{
		if (size < 0)
		{
			throw new IllegalArgumentException("size < 0");
		}

		Block block = tryGetAllocatedBlock(key);
		if (null == block)
		{
			// System.out.println("realloc key not found");
			return copy ? BytePool.NULL : alloc(size);
		}

		if (block.dataLength() >= size)
		{
			return key;
		}

		Block newBlock = doAlloc(size);
		if (copy)
		{
			blockCopy(block, newBlock);
		}

		freeBlock(block);
		return newBlock.getPos();

	}

	// Note:但你越界使用你所分配的Block空间的时候，会抛出免检异常
	// BlockIndexOutOfBoundsException，以反映编码错误。但是，
	// 极少情况下，会因为磁盘上的Block头信息损坏，并且通过了冗余检测，改变了
	// Block的长度。此时通过正确方式使用Block时，可能会因为错误检测代码受到Block长度
	// 错误的影响，抛出免检异常BlockIndexOutOfBoundsException。总之，这是一个因为IO
	// 异常而抛出免检异常的情况。如果你追求极高的健壮性(变态高更恰当)，可以考虑处理这个异常。
	// 上述情况发生的可能性极小，它需要首先Block的头信息发生突变，然后冗余码发生突变
	// 然后两者突变后还要相匹配以通过冗余检测。
	@Override
	public boolean get(long key, byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		Block block = tryGetAllocatedBlock(key);
		if (null == block)
		{
			return false;
		}

		// System.out.println(Thread.currentThread() + "pool.get: block=" +
		// block);
		block.get(buf, bufOffset, idx, length);
		return true;
	}

	@Override
	public boolean set(long key, byte[] buf, int bufOffset, long idx, int length)
			throws IOException
	{
		Block block = tryGetAllocatedBlock(key);
		if (null == block)
		{
			return false;
		}

		// System.out.println(Thread.currentThread() + "pool.set: block=" +
		// block);
		block.set(buf, bufOffset, idx, length);
		return true;
	}

	// =============================================================

	// 考虑加入checkValid方法(现在不需要了2010-8-10)

	// 测试的时候，将MAX_LEN_EXP降低到可测试的程度，测试临界大块
	// 当代码完工时，从整体上审阅Pool的逻辑

	// 测试时需要测试随机访问任意位置pos，看看是否会抛出免检异常
	// 测试同步的时候，应该使用高频分配的测试，过去的测试会因为读写任务大，
	// 每次分配时间间隔长而难以引起并发错误。
	// 非常重要，想办法检查自由链表块丢失的问题
	// 非常重要，想办法检查自由链表重新利用块的问题，别到时每次都新建块
	// 非常重要，检查是否会将header==0-0误认为块
	// 记得测试pool的大小扩张
	// 记得测试pool的大小缩减
	// 记得测试和前作的性能比较
}
