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

// ͨ����β����������飬����һ�������飬�������������������ʱ��
// ���̿ռ���Ƭ�޷����õ����⡣
// �ṹ��SegmentArray�������Segment��һ��Segment�������Element��һ��Element��һ���ֽ�
// �����ԣ���Ϊ���������飬���get/set����Ȼ���Բ����ģ����ǽṹ�ĸı䣬����add/remove segment
// ����Ҫ�����ͬ��
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
					+ segIdx); // TODO ������ɺ�ע�͵�
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

	// TODO fillϵ�з���ҪС�ĵؼ���fill�ı߽��Ƿ���ȷ
	// TODO ���Կ��ǸĽ���Ϊ����elementΪ��λ������֮��Ľ�HashMap��create�����У���ʼ����ϣ��Ĳ���
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
						+ segIdx); // TODO ������ɺ�ע�͵�
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

	// TODO ���ų��ȴ�С��һ�鵽����ȫ������ȫ��0�����������1���Լ��߽��Ƿ���
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

	// TODO �����Ƿ��ͷ������пռ�
	@Override
	public void clear() throws IOException
	{
		while (this.getSegmentNum() > 0)
		{
			removeSegment(this.getSegmentNum() - 1);
		}
	}

	// TODO �����ٽ��С������segmentSize==1��elementLength==1��
}
