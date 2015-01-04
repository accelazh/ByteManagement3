package org.accela.bm.collections.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.accela.bm.collections.common.ElementContainer;
import org.accela.bm.common.Concurrentable;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.common.Persistancer;
import org.accela.bm.pool.BytePool;
import org.accela.bm.pool.TolerantBytePool;

// 通过多段不连续的数组，构成一个大数组，解决连续数组扩增容量时，
// 磁盘空间碎片无法利用的问题。
// 结构：SegmentArray包含多个Segment，一个Segment包含多个Element，一个Element是一段字节
// 并发性：因为本质是数组，因此get/set是天然可以并发的，但是结构的改变，比如add/remove segment
// 就需要额外的同步
public class SegmentArray implements Persistancer, ElementContainer,
		Concurrentable
{
	private TolerantBytePool pool = null;

	private long elementLength = 0;

	private long segmentSize = 0;

	private CopyOnWriteArrayList<Long> segments = null;

	public SegmentArray(BytePool pool, long elementLength, long segmentSize)
	{
		if (null == pool)
		{
			throw new IllegalArgumentException("null pool");
		}
		if (elementLength < 1)
		{
			throw new IllegalArgumentException("illegal elementLength: "
					+ elementLength);
		}
		if (segmentSize < 1)
		{
			throw new IllegalArgumentException("illegal segmentSize: "
					+ segmentSize);
		}

		this.pool = new TolerantBytePool(pool);
		this.elementLength = elementLength;
		this.segmentSize = segmentSize;
	}

	public BytePool getPool()
	{
		return this.pool.getPool();
	}

	@Override
	public long getElementLength()
	{
		return this.elementLength;
	}

	public long getSegmentSize()
	{
		return this.segmentSize;
	}

	public int getSegmentNum()
	{
		return segments.size();
	}

	public long length()
	{
		return getSegmentSize() * getSegmentNum();
	}

	@Override
	public long size()
	{
		return length();
	}

	// ====================================================================

	private void initSegments(int segmentNum) throws IOException
	{
		if (segmentNum < 0)
		{
			throw new IllegalArgumentException("illegal initSegmentNun: "
					+ segmentNum);
		}

		segments = new CopyOnWriteArrayList<Long>();
		for (int i = 0; i < segmentNum; i++)
		{
			this.insertSegment(getSegmentNum());
		}
	}

	private void initSegments(List<Long> segmentKeys)
	{
		segments = new CopyOnWriteArrayList<Long>();
		segments.addAll(segmentKeys);
	}

	// args[0]: initSegmentNum
	@Override
	public void create(Object... args) throws IOException
	{
		int initSegmentNum = (args != null && args[0] instanceof Integer) ? (Integer) args[0]
				: 0;
		initSegments(initSegmentNum);
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		int segmentNum = in.readInt();
		List<Long> segmentKeys = new LinkedList<Long>();
		for (int i = 0; i < segmentNum; i++)
		{
			segmentKeys.add(in.readLong());
		}

		try
		{
			initSegments(segmentKeys);
		}
		catch (RuntimeException ex)
		{
			throw new DataFormatException(ex);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(segments.size());
		for (Long key : segments)
		{
			out.writeLong(key);
		}
	}

	// ================================================================

	public void insertSegment(int segIdx) throws IOException
	{
		if (segIdx < 0 || segIdx > getSegmentNum())
		{
			throw new IllegalArgumentException("illegal segIdx: " + segIdx);
		}

		long key = pool.alloc(getElementLength() * getSegmentSize());
		segments.add(segIdx, key);
	}

	public void removeSegment(int segIdx) throws IOException
	{
		if (segIdx < 0 || segIdx >= getSegmentNum())
		{
			throw new IllegalArgumentException("illegal segIdx: " + segIdx);
		}

		long key = segments.remove(segIdx);
		pool.free(key);
	}

	private void resetSegment(int segIdx, boolean removeOld) throws IOException
	{
		if (segIdx < 0 || segIdx >= getSegmentNum())
		{
			throw new IllegalArgumentException("illegal segIdx: " + segIdx);
		}

		long oldKey = segments.set(segIdx,
				pool.alloc(getElementLength() * getSegmentSize()));
		if (removeOld)
		{
			pool.free(oldKey);
		}
	}

	public void addSegment() throws IOException
	{
		this.insertSegment(getSegmentNum());
	}

	// ================================================================

	public int getSegmentIndex(long elementIdx)
	{
		if (elementIdx < 0)
		{
			throw new IllegalArgumentException("illegal elementIdx: "
					+ elementIdx);
		}

		long segIdx = (elementIdx / getSegmentSize());
		if (segIdx > Integer.MAX_VALUE)
		{
			throw new IllegalArgumentException("elementIdx illegal: "
					+ elementIdx);
		}

		return (int) segIdx;
	}

	public long getElementIndexInSegment(long elementIdx)
	{
		if (elementIdx < 0)
		{
			throw new IllegalArgumentException("illegal elementIdx: "
					+ elementIdx);
		}

		return elementIdx % getSegmentSize();
	}

	public long getElementIndex(int segIdx, long elementIdxInSeg)
	{
		if (segIdx < 0)
		{
			throw new IllegalArgumentException("illegal segIdx: " + segIdx);
		}
		if (elementIdxInSeg < 0)
		{
			throw new IllegalArgumentException("illegal elementIdxInSeg: "
					+ elementIdxInSeg);
		}

		return segIdx * getSegmentSize() + elementIdxInSeg;
	}

	// ================================================================

	public void getElement(int segIdx,
			long elementIdxInSeg,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (segIdx < 0 || segIdx >= getSegmentNum())
		{
			throw new IndexOutOfBoundsException("segIdx illegal" + segIdx);
		}
		if (elementIdxInSeg < 0 || elementIdxInSeg >= getSegmentSize())
		{
			throw new IndexOutOfBoundsException("elementIdxInSeg illegal"
					+ elementIdxInSeg);
		}
		if (idxInElement < 0
				|| length < 0
				|| length + idxInElement > getElementLength())
		{
			throw new IndexOutOfBoundsException(
					"idxInElement or length illegal"
							+ idxInElement
							+ ", "
							+ length);
		}

		long segmentKey = segments.get(segIdx);
		pool.get(segmentKey, buf, bufOffset, elementIdxInSeg
				* getElementLength()
				+ idxInElement, length);
	}

	public void setElement(int segIdx,
			long elementIdxInSeg,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		if (segIdx < 0 || segIdx >= getSegmentNum())
		{
			throw new IndexOutOfBoundsException("segIdx illegal" + segIdx);
		}
		if (elementIdxInSeg < 0 || elementIdxInSeg >= getSegmentSize())
		{
			throw new IndexOutOfBoundsException("elementIdxInSeg illegal"
					+ elementIdxInSeg);
		}
		if (idxInElement < 0
				|| length < 0
				|| length + idxInElement > getElementLength())
		{
			throw new IndexOutOfBoundsException(
					"idxInElement or length illegal"
							+ idxInElement
							+ ", "
							+ length);
		}

		long segmentKey = segments.get(segIdx);
		boolean succ = pool.set(segmentKey, buf, bufOffset, elementIdxInSeg
				* getElementLength()
				+ idxInElement, length);

		if (!succ)
		{
			System.err.println("SegmentArray.setElement "
					+ "encountered a lost segment error: "
					+ "segIdx="
					+ segIdx); // TODO 测试完成后注释掉
			resetSegment(segIdx, false);
			setElement(segIdx,
					elementIdxInSeg,
					buf,
					bufOffset,
					idxInElement,
					length);
		}

	}

	public void getElement(long elementIdx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		this.getElement(this.getSegmentIndex(elementIdx),
				this.getElementIndexInSegment(elementIdx),
				buf,
				bufOffset,
				idxInElement,
				length);
	}

	public void setElement(long elementIdx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException
	{
		this.setElement(this.getSegmentIndex(elementIdx),
				this.getElementIndexInSegment(elementIdx),
				buf,
				bufOffset,
				idxInElement,
				length);
	}

	// =====================================================================

	// TODO fill系列方法要小心地检验fill的边界是否正确
	// TODO 可以考虑改进成为按照element为单位来做，之后改进HashMap的create方法中，初始化哈希表的部分
	public void fill(int segIdx,
			long startElementIdxInSeg,
			long elementNum,
			byte value) throws IOException
	{
		if (segIdx < 0 || segIdx >= getSegmentNum())
		{
			throw new IndexOutOfBoundsException("segIdx illegal" + segIdx);
		}
		if (startElementIdxInSeg < 0
				|| elementNum < 0
				|| startElementIdxInSeg + elementNum > getSegmentSize())
		{
			throw new IndexOutOfBoundsException(
					"startElementIdxInSeg or elementNum illegal: "
							+ startElementIdxInSeg
							+ ", "
							+ elementNum);
		}

		final long start = startElementIdxInSeg * getElementLength();
		final long len = elementNum * getElementLength();
		final long segKey = segments.get(segIdx);
		final int BUF_LEN = 1024 * 8;

		byte[] buf = new byte[(int) Math.min(len, BUF_LEN)];
		Arrays.fill(buf, value);

		long count = 0;
		while (count < len)
		{
			int step = (int) Math.min(buf.length, len - count);
			boolean succ = pool.set(segKey, buf, 0, start + count, step);
			if (!succ)
			{
				System.err.println("SegmentArray.fill(...) "
						+ "encountered a lost segment error: "
						+ "segIdx="
						+ segIdx); // TODO 测试完成后注释掉
				resetSegment(segIdx, false);
				this.fill(segIdx, startElementIdxInSeg, elementNum, value);

				return;
			}
			count += step;
		}
		assert (len == count);
		assert (count / getElementLength() == elementNum);
	}

	public void fill(int segIdx, byte value) throws IOException
	{
		this.fill(segIdx, 0, getSegmentSize(), value);
	}

	public void fill(byte value) throws IOException
	{
		for (int segIdx = 0; segIdx < getSegmentNum(); segIdx++)
		{
			this.fill(segIdx, value);
		}
	}

	// TODO 检查才长度从小与一块到覆盖全部，在全是0的数组上填充1，以检查边界是否超了
	public void fill(long elementIdx, long elementNum, byte value)
			throws IOException
	{
		if (elementIdx < 0
				|| elementNum < 0
				|| elementIdx + elementNum > length())
		{
			throw new IndexOutOfBoundsException(
					"elementIdx or elementNum illegal: "
							+ elementIdx
							+ ", "
							+ elementNum);
		}

		long curNum = 0;
		while (curNum < elementNum)
		{
			long curIdx = elementIdx + curNum;
			int segIdx = getSegmentIndex(curIdx);
			long curIdxInSeg = getElementIndexInSegment(curIdx);
			long step = Math.min(elementNum - curNum, getSegmentSize()
					- curIdxInSeg);

			this.fill(segIdx, curIdxInSeg, step, value);
			curNum += step;
		}
		assert (curNum == elementNum);
	}

	// TODO 测试是否释放了所有空间
	@Override
	public void clear() throws IOException
	{
		while (this.getSegmentNum() > 0)
		{
			removeSegment(this.getSegmentNum() - 1);
		}
	}

	// TODO 测试临界大小，比如segmentSize==1、elementLength==1等
}
