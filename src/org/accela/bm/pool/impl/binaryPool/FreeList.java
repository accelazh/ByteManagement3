package org.accela.bm.pool.impl.binaryPool;

import java.io.IOException;

import org.accela.bm.pool.ByteAccesser;
import org.accela.bm.pool.BytePool;

public class FreeList
{
	// 测试时，所有出错处皆放上打印语句
	private long headPos = BytePool.NULL; // Note：-1表示NULL地址

	private byte lenExp = Common.MIN_LEN_EXP;

	private ByteAccesser accesser = null;

	// NOTE: 这里的缓冲策略是，读可以从缓冲中读，写的时候同步更新缓冲和磁盘，
	// 为什么写时不缓冲？因为headBlock是关键数据，如果没有及时写回磁盘
	// 就丢失，会造成链表丢失。
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

	// Note: 注意与headPreload的冲突(脏读、脏写)
	// NOTE: 传入参数的pointer应该已经被load了
	public synchronized boolean remove(Block block, PoolSize poolSize)
			throws IOException
	{
		// FreeList remove方法的改进，使得其不可重复
		// Block的getbuddy等方法的取块，以及其它取块方法全部移入Common.getBlock中
		// NULL值的建立
		// 这个方法自读/写pointer，通过pointer和headPos使得不能重复删除块，让该方法具有同步控制的功能。
		// 加入test头部和next的方法，防止重复put块
		// 重要约定：1. 块的read、write因其free仅1bit，具有原子性，不过如果读到broken会怎样需要斟酌
		// 2. 一个块被拿到时读，使用完毕时写，free发生转换，并且只有在使用完毕写完时，才free转换。
		// 读、写作为同步控制的基础之一，必须在最开头和最结尾
		// 3. freeList的不可重复删除性和自身的同步，构成取块和放块的同步机制的基础，保证：
		// free-free打架的避免和alloc-free打架的避免。free-free打架只是还可能导致放块不能一定
		// 上滚到最大。nonfreeBlock以及整个同步过程的细节仍然需要斟酌
		// 连续free两次同一块，可能造成的问题，考虑merge，还需斟酌
		// 一个几乎不用锁的高度并行的同步方案已经浮现

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

		succ = block.loadHeader(); // 传入的block可能已经过时(在block被load之后，上面检测inFreeListSign之前，这个block可能被另一个线程取出，然后merge/divide以改变lenExp，然后放入FreeList组。这样导致block的lenExp过时。)
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
