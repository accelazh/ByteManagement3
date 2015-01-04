package org.accela.bm.pool.impl.binaryPool;

import java.io.IOException;

import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.BytePool;

public class FreeList
{
	// ����ʱ�����г����Է��ϴ�ӡ���
	private long headPos = BytePool.NULL; // Note��-1��ʾNULL��ַ

	private byte lenExp = Common.MIN_LEN_EXP;

	private ByteAccesser accesser = null;

	// NOTE: ����Ļ�������ǣ������Դӻ����ж���д��ʱ��ͬ�����»���ʹ��̣�
	// Ϊʲôдʱ�����壿��ΪheadBlock�ǹؼ����ݣ����û�м�ʱд�ش���
	// �Ͷ�ʧ�����������ʧ��
	private Block headPreload = null;

	public FreeList(long headPos, byte lenExp, ByteAccesser accesser)
	{
		if (headPos < BytePool.NULL)
		{
			throw new IllegalArgumentException("headPos illegal");
		}
		if (!Common.checkLenExp(lenExp))
		{
			throw new IllegalArgumentException("lenExp illegal");
		}
		if (null == accesser)
		{
			throw new IllegalArgumentException("null accesser");
		}

		this.headPos = headPos;
		this.lenExp = lenExp;
		this.accesser = accesser;
	}

	public synchronized long getHeadPos()
	{
		assert (headPos >= BytePool.NULL);
		return headPos;
	}

	public byte getLenExp()
	{
		assert (Common.checkLenExp(lenExp));
		return this.lenExp;
	}

	public ByteAccesser getAccesser()
	{
		return this.accesser;
	}

	private boolean checkBlock(Block block)
	{
		assert (block != null);

		if (block.getLenExp() != this.getLenExp())
		{
			// System.out.println("Freelist check block failure");
			return false;
		}

		assert (block.getAccesser() == this.getAccesser());
		return true;
	}

	private Block tryGetFreeListBlock(long pos, PoolSize poolSize)
			throws IOException
	{
		Block block = Common.tryGetBlock(pos, getLenExp(), poolSize, accesser);
		if (null == block)
		{
			// System.out
			// .print(pos != BytePool.NULL ?
			// "FreeList.tryGetFreeListBlock error 1\n"
			// : "");
			return null;
		}
		block.loadPointer();
		if (!checkBlock(block))
		{
			// System.out.println("FreeList.tryGetFreeListBlock error 2");
			return null;
		}
		if (!block.isFree())
		{
			// System.out.println("FreeList.tryGetFreeListBlock error 3");
			return null;
		}
		boolean succ = block.loadInFreeListSign();
		if (!succ || !block.isInFreeList())
		{
			// System.out.println("FreeList.tryGetFreeListBlock error 4");
			return null;
		}

		return block;
	}

	private void preloadHead(PoolSize poolSize) throws IOException
	{
		if (null == this.headPreload)
		{
			this.headPreload = tryGetFreeListBlock(this.headPos, poolSize);
		}
		else
		{
			// System.out.println("Freelist preload head hit");
		}
	}

	public synchronized Block retrieve(PoolSize poolSize) throws IOException
	{
		preloadHead(poolSize);
		Block headBlock = this.headPreload;
		if (null == headBlock)
		{
			headPos = BytePool.NULL;
			return null;
		}
		headPos = BytePool.NULL;
		headPreload = null;

		Block nextBlock = tryGetFreeListBlock(headBlock.getNext(), poolSize);
		if (null == nextBlock)
		{
			headPos = BytePool.NULL;
			headPreload = null;
		}
		else
		{
			headPos = nextBlock.getPos();
			headPreload = nextBlock;

			nextBlock.setPrev(BytePool.NULL);
			nextBlock.savePointer();
		}

		headBlock.setInFreeList(false);
		headBlock.saveInFreeListSign();

		assert (checkBlock(headBlock));
		assert (headBlock.isFree());
		assert (!headBlock.isInFreeList());
		assert (headBlock.getPos() >= 0 && headBlock.getPos() < poolSize
				.getValue());
		// System.out.println(Thread.currentThread()
		// + "FreeList: retrieve from free list, free list lenExp = "
		// + getLenExp()
		// + ", block = "
		// + headBlock);
		return headBlock;
	}

	private Block tryGetFreeBlockWithPreload(long pos, PoolSize poolSize)
			throws IOException
	{
		if (pos == headPos)
		{
			preloadHead(poolSize);
			return headPreload;
		}

		return tryGetFreeListBlock(pos, poolSize);
	}

	// Note: ע����headPreload�ĳ�ͻ(�������д)
	// NOTE: ���������pointerӦ���Ѿ���load��
	public synchronized boolean remove(Block block, PoolSize poolSize)
			throws IOException
	{
		// FreeList remove�����ĸĽ���ʹ���䲻���ظ�
		// Block��getbuddy�ȷ�����ȡ�飬�Լ�����ȡ�鷽��ȫ������Common.getBlock��
		// NULLֵ�Ľ���
		// ��������Զ�/дpointer��ͨ��pointer��headPosʹ�ò����ظ�ɾ���飬�ø÷�������ͬ�����ƵĹ��ܡ�
		// ����testͷ����next�ķ�������ֹ�ظ�put��
		// ��ҪԼ����1. ���read��write����free��1bit������ԭ���ԣ������������broken��������Ҫ����
		// 2. һ���鱻�õ�ʱ����ʹ�����ʱд��free����ת��������ֻ����ʹ�����д��ʱ����freeת����
		// ����д��Ϊͬ�����ƵĻ���֮һ���������ͷ�����β
		// 3. freeList�Ĳ����ظ�ɾ���Ժ������ͬ��������ȡ��ͷſ��ͬ�����ƵĻ�������֤��
		// free-free��ܵı����alloc-free��ܵı��⡣free-free���ֻ�ǻ����ܵ��·ſ鲻��һ��
		// �Ϲ������nonfreeBlock�Լ�����ͬ�����̵�ϸ����Ȼ��Ҫ����
		// ����free����ͬһ�飬������ɵ����⣬����merge����������
		// һ�������������ĸ߶Ȳ��е�ͬ�������Ѿ�����

		if (null == block)
		{
			throw new IllegalArgumentException("block should not be null");
		}
		if (null == poolSize)
		{
			throw new IllegalArgumentException("null poolSize");
		}
		if (!checkBlock(block))
		{
			throw new IllegalArgumentException("illegal block");
		}
		if (!block.isFree())
		{
			throw new IllegalArgumentException("not free block");
		}

		// check the block belongs to the free list
		boolean succ = block.loadInFreeListSign();
		if (!succ || !block.isInFreeList())
		{
			return false;
		}

		succ = block.loadHeader(); // �����block�����Ѿ���ʱ(��block��load֮��������inFreeListSign֮ǰ�����block���ܱ���һ���߳�ȡ����Ȼ��merge/divide�Ըı�lenExp��Ȼ�����FreeList�顣��������block��lenExp��ʱ��)
		if (!succ || block.getLenExp() != getLenExp())
		{
			return false;
		}

		block.loadPointer();

		// check block equals to headPreload
		if (block.getPos() == headPos)
		{
			headPreload = block;
			block.setPrev(BytePool.NULL);
		}

		// load next block and modify it
		Block nextBlock = tryGetFreeBlockWithPreload(block.getNext(), poolSize);
		if (nextBlock != null && nextBlock.getPrev() == block.getPos())
		{
			nextBlock.setPrev(block.getPrev());
			nextBlock.savePointer();
		}
		else
		{
			if (nextBlock != null)
			{
				// System.out.println("FreeList remove block: broken mid block");
			}
		}

		// load prev block and modify it
		Block prevBlock = tryGetFreeBlockWithPreload(block.getPrev(), poolSize);
		if (prevBlock != null && prevBlock.getNext() == block.getPos())
		{
			prevBlock.setNext(block.getNext());
			prevBlock.savePointer();
		}
		else
		{
			if (prevBlock != null)
			{
				// System.out.println("FreeList remove block: broken mid block");
			}
		}

		// check if head is removed
		if (block.getPos() == headPos)
		{
			headPos = (null == nextBlock) ? BytePool.NULL : nextBlock.getPos();
			headPreload = nextBlock;
		}

		// save block's inFreeListSign
		block.setInFreeList(false);
		block.saveInFreeListSign();

		// System.out.println(Thread.currentThread()
		// + "FreeList: remove from free list, free list lenExp = "
		// + getLenExp()
		// + ", block = "
		// + block);
		return true;
	}

	public synchronized boolean put(Block block, PoolSize poolSize)
			throws IOException
	{
		if (null == block)
		{
			throw new IllegalArgumentException("block should not be null");
		}
		if (null == poolSize)
		{
			throw new IllegalArgumentException("null poolSize");
		}
		if (!checkBlock(block))
		{
			throw new IllegalArgumentException("illegal block");
		}

		if (block.getPos() == headPos)
		{
			return false;
		}
		boolean succ = block.loadInFreeListSign();
		if (!succ || block.isInFreeList())
		{
			return false;
		}

		preloadHead(poolSize);
		Block headBlock = headPreload;
		if (headBlock != null && headBlock.getNext() == block.getPos())
		{
			return false;
		}

		if (null == headBlock)
		{
			block.setNext(BytePool.NULL);
		}
		else
		{
			block.setNext(headBlock.getPos());
			headBlock.setPrev(block.getPos());

			headBlock.savePointer();
		}

		long oldHeadPos = headPos;
		Block oldHeadPreload = headPreload;

		block.setFree(true);
		block.setInFreeList(true);
		block.setPrev(BytePool.NULL);

		headPos = block.getPos();
		headPreload = block;

		try
		{
			block.savePointer();
			block.saveInFreeListSign();
			block.saveHeader();
		}
		catch (IOException ex)
		{
			headPos = oldHeadPos;
			headPreload = oldHeadPreload;

			headBlock.setPrev(BytePool.NULL);
			headBlock.savePointer();

			return false;
		}

		// System.out.println(Thread.currentThread()
		// + "FreeList: put to free list, free list lenExp = "
		// + getLenExp()
		// + ", block = "
		// + block);
		return true;
	}

}
