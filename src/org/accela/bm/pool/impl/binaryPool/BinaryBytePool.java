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
 * ���˼·��
 * 1.Block�ĳ���Ϊ����2���ݡ����������Block���Ȼ��ƣ�ʹ��һϵ������ļ����ܹ�ʹ��
 * ��Block�ĳ��Ƚ���1�ֽڱ�ʾ������У�������2�ֽڣ�����ʡ�ռ䣻Block�ĳ��Ⱥ͵�ַ
 * ��ƥ������ܹ����У�Block�ķָ���ϲ��򵥣������ܹ�֧�ִ�32�ֽڵ�2^60���ϵĴ�С��
 * �ȵȣ���
 * 2.Block�ķ��䣬�ܹ��ָ��Block��Block�ͷ�ʱ������������ܵ�Block�ϲ��ɸ����Block��
 * 3.�صĴ�С�ܹ��Զ������������ǣ����ص���ʹ�ÿռ�Ϊ���ܴ�С��1/4ʱ���صĴ�С������
 * ԭ����1/2��������Կ����˳ش�С�����������Ŀ�������
 * 
 * Features: 
 * 1. Block������У����ƣ�ʹ���ܹ�ʶ�𻵿顣
 * 2. ���ʹ�ô����key������Block������У������Լ�Block���Ⱥ͵�ַ��ƥ�����
 * 	     �ܹ��ڶ�������·��ִ���
 * 3. Block����Խ����
 * 3. FreeListʹ�����������ͷ�ļ��������ٴ��̷��ʡ�
 * 4. alloc��free���̣����̷��ʱ��Ż���Block��header��Ϣ�����������̵Ŀ�ͷ������βд��
 * ����IO������
 * 5. �߶ȵĿɿ��ԡ�Block�𻵡����������𻵣���������𻵻�����ʧ�Ĵ��̿ռ��޷�ʹ�ã�
 * ����Ȼ�ܹ�������������ʹ���������Ĩ�������õ��ļ�������Ȼ�ܹ�����������ֻ�Ǳ�Ĩ����
 * ���ݻᶪʧ����Ĩ���Ŀռ�����޷����á�
 * 6. �������Ʊ��Ż���alloc��free��get/set��expand/shrink�������Բ��й�����������������
 * �����Բ��й�����������һЩ���ƣ�����ὲ��
 * 7. �μ�Block.getBuddy()������һ��block���ܹ��������merge��block���ҽ���һ�顣�⹹��
 * �˶��ַ�merge/divide�Ĺؼ����Զ��߳�ģ��Ҳ�������֮һ��
 * 
 * 
 * ����ģ�ͣ�
 * 0. �μ�Features.7��
 * 1. ÿ��FreeList�Լ������Լ���ͬ����FreeList��������Block�Ĳ������ơ�
 * 2. alloc��free��ʹ�ÿ���Blockʱ��һ��ͨ��FreeList��FreeList��֤ͬһ������Block���ᱻ
 * ����̳߳��С���FreeList��remove������retrieve��������֤ͬһ��Block���ܹ���remove����
 * retrieve���Σ�
 * 3. Block��header�Ķ���д����Ϊ������Ĵ洢��ʹ�÷�ʽ����֤Block��header�����
 * д��һ�룬��ȡ�����ᱻ��Ϊbroken��Block�𻵣����Ӷ��޷�ʹ�á���˵���ǣ����Ƶ�
 * ԭ���ԡ��ڶ���alloc��free����ֻ�������� ���޸ĵ�Block��header��Ϣ���浽���̣�
 * �Ӷ��ﵽ���Ч����alloc��free�ȷ����ڲ���һ��Block��δ���ʱ����Block��header�޷�
 * ����һ���̵߳�alloc��free������ʹ�á�
 * 4. �صĴ�С���ź������Ĺ��̣���Ҫ����ͬ������Щ�����޸�poolSize��С�����poolSize
 * ���޸ĵ�˳���ܹ�����alloc��free�ȿɼ��ĳ�����Ϊ���ܳش�С�仯Ӱ������򣬴Ӷ�
 * ʵ�ֳش�С�仯��alloc��free�����̵Ĳ�����
 * 5. ���ڶ����ѷ����Blockû�в������ư취����˲���ģ����һ�����ƣ�������
 * 6. ��������������������������Ⲣ�����ƣ�������֣������˰�����ء����ڵķ���
 * ������෽���ĶԱȺ��ݻ��е����ģ�����˼������󻯲����������Ϣ���Լ��ҵ�˼������
 * ��û�б�д������Ҫ����������Ҫ���Լ�˼������Ƴ�һ�ײ���������Ȼ���������ҵĶԱȣ�
 * ��������������û��д��������ơ�
 * 
 * =========================================================================
 * ����ģ�͵����ƣ�
 * 1. ������һ��Block��free����δ���ʱ���ٴ�free���Block��
 * 2. ������һ��Block��free����δ���ʱ��get/set���Block��
 * =========================================================================
 * 
 * ȱ��:
 * 1. Block��С����2��ָ�������У�����ʱ�൱�˷ѿռ䣬����ռ�ÿռ��������Ч�ռ������
 * 2. Blockһ���ͷžͳ���merge������ʱBlock�ȳ���divide��merge��divide���ܻ��γɶ���
 * 3. ���߳��£�Ϊ��֤�����ԣ�Block��merge�������ܲ���֣��Ӷ��ش�С��shrink���ܲ���֡�
 * �������ǣ����Block A mergeʱ��ҪB��Block B mergeʱ��ҪA��A��B�����ʱ���ھ���
 * free������ܵ��£�A���B�Ƿ��������mergeʱ��B��free���������е�һ�룬���ʧ�ܣ�B��A
 * ͬ�����A��B������merge������A��B��free���̽�����ȴ��Ӧ��merge�ġ�
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

	// NOTE: ���з���Block�ķ�����Blockһ��δ�洢������Block���벢�����صķ���
	// һ������Block����ʱ�Ѿ����ع�
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

	// ����һ��Block�����ڽ���ͬ������֮ǰ�����޸���
	private Block peekBlock(long pos) throws IOException
	{
		return Common.tryGetBlock(pos, poolSize, accesser);
	}

	// ����һ��Block�����ڽ���ͬ������֮ǰ�����޸���
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
		// ��һ���ĳɹ���֤merge������ռbuddy���Ӷ���������߳�ͬʱʹ��һ��Block
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

	// NOTE: ��Block�Ĵ�С��poolSizeһ�����ʱ��merge������EOFException
	private Block firstExpand(byte lenExp) throws IOException
	{
		// System.out.println("firstExpand: lenExp=" + lenExp);

		assert (Common.checkLenExp(lenExp));
		assert (poolSize.isZero());

		accesser.setLength(Common.expToValue(lenExp));

		Block block = new Block(0, accesser);
		block.setLenExp(lenExp);

		block.setFree(true);
		block.saveHeader(); // ��ֹBlock����free
		block.setInFreeList(false);
		block.saveInFreeListSign(); // ��ֹBlock����FreeList.remove

		// ͬ������:ֻ�е�expand���ʱ���Ÿı�poolSize��ֵ
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
		block.saveHeader(); // ��ֹBlock����free
		block.setInFreeList(false);
		block.saveInFreeListSign(); // ��ֹBlock����FreeList.remove

		// ͬ������:ֻ�е�expand���ʱ���Ÿı�poolSize��ֵ
		poolSize.setDouble();
		assert (accesser.length() == poolSize.getValue());
		return block;
	}

	// NOTE: �ռ����Ƿ���null�����ص�Block�Ĵ�С���ܴ���lenExp
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

	// �ص��°�ν������shrink
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

	// �ص��ϰ�λ�����1/4�λ����ϰ�ε��°�ν������shrink
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
		// ��һ���ĳɹ���֤shrink������ռtest���Ӷ���������߳�ͬʱʹ��һ��Block
		boolean succRemoved = removeFromFreeList(test);
		if (!succRemoved)
		{
			return false;
		}

		shrinkHalf();
		return true;
	}

	// Note: �������blockӦ�û�û�б�������������
	// ����ֵΪ{�Ƿ�shrink�ˣ�����block�Ƿ�shrink����}
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

	// NOTE: ���������Ȼ����Block�����ڷ���ǰ�Ѿ�������Block����Ϣ
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

	// Note:����Խ��ʹ�����������Block�ռ��ʱ�򣬻��׳�����쳣
	// BlockIndexOutOfBoundsException���Է�ӳ������󡣵��ǣ�
	// ��������£�����Ϊ�����ϵ�Blockͷ��Ϣ�𻵣�����ͨ���������⣬�ı���
	// Block�ĳ��ȡ���ʱͨ����ȷ��ʽʹ��Blockʱ�����ܻ���Ϊ����������ܵ�Block����
	// �����Ӱ�죬�׳�����쳣BlockIndexOutOfBoundsException����֮������һ����ΪIO
	// �쳣���׳�����쳣������������׷�󼫸ߵĽ�׳��(��̬�߸�ǡ��)�����Կ��Ǵ�������쳣��
	// ������������Ŀ����Լ�С������Ҫ����Block��ͷ��Ϣ����ͻ�䣬Ȼ�������뷢��ͻ��
	// Ȼ������ͻ���Ҫ��ƥ����ͨ�������⡣
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

	// ���Ǽ���checkValid����(���ڲ���Ҫ��2010-8-10)

	// ���Ե�ʱ�򣬽�MAX_LEN_EXP���͵��ɲ��Եĳ̶ȣ������ٽ���
	// �������깤ʱ��������������Pool���߼�

	// ����ʱ��Ҫ���������������λ��pos�������Ƿ���׳�����쳣
	// ����ͬ����ʱ��Ӧ��ʹ�ø�Ƶ����Ĳ��ԣ���ȥ�Ĳ��Ի���Ϊ��д�����
	// ÿ�η���ʱ���������������𲢷�����
	// �ǳ���Ҫ����취�����������鶪ʧ������
	// �ǳ���Ҫ����취������������������ÿ�����⣬��ʱÿ�ζ��½���
	// �ǳ���Ҫ������Ƿ�Ὣheader==0-0����Ϊ��
	// �ǵò���pool�Ĵ�С����
	// �ǵò���pool�Ĵ�С����
	// �ǵò��Ժ�ǰ�������ܱȽ�
}
