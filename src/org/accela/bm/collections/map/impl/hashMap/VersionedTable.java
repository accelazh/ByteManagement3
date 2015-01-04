package org.accela.bm.collections.map.impl.hashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.accela.bm.collections.common.ElementViewer;
import org.accela.bm.collections.map.impl.hashMap.RawTable.Node;
import org.accela.bm.collections.map.impl.hashMap.RawTable.SlotIterator;
import org.accela.bm.common.Common;
import org.accela.bm.common.Persistancer;

/*
 * ����Ϊ��ϣ��ÿ����һ�����ţ��Ͳ�����һ���µ�Version�Ĺ�ϣ��
 * ���ܸ����汾�ı��ײ㹫����ͬ��RawTable������������ʹ�õ�
 * tableSize������ͬ����һ��Version����һ��Version��������
 * ��Ϊ�ڴӾɱ����±��ǨԪ��ʱ���¾ɱ���Ҫ���棬���ʹ��Version
 * �����������¾ɱ��Ƚ����ס�
 */
public class VersionedTable implements Persistancer
{
	private RawTable table = null;

	private long tableSize = 0;

	private ScanIdx formatterIdx = null;

	private ScanIdx moverIdx = null;

	public VersionedTable(RawTable table, long tableSize) throws IOException
	{
		if (null == table)
		{
			throw new NullPointerException("table is null");
		}
		if (tableSize < 0 || tableSize > table.tableSize())
		{
			throw new IllegalArgumentException("tableSize illegal");
		}
		if (Common.valueToExpValue(tableSize) != tableSize)
		{
			throw new IllegalArgumentException("tableSize illegal");
		}

		this.table = table;
		this.tableSize = tableSize;
	}

	public RawTable getTable()
	{
		return table;
	}

	public long tableSize()
	{
		return tableSize;
	}

	public ScanIdx formatterIdx()
	{
		return formatterIdx;
	}

	public ScanIdx moverIdx()
	{
		return moverIdx;
	}

	@Override
	public void create(Object... args) throws IOException
	{
		this.formatterIdx = new ScanIdx(tableSize / 2, tableSize);
		this.moverIdx = new ScanIdx(0, tableSize);
	}

	@Override
	public void read(DataInput in) throws IOException
	{
		try
		{
			this.formatterIdx = new ScanIdx(in.readLong(), in.readLong(),
					in.readLong());
		}
		catch (RuntimeException ex)
		{
			this.formatterIdx = new ScanIdx(tableSize / 2, tableSize);
		}
	
		try
		{
			this.moverIdx = new ScanIdx(in.readLong(), in.readLong(),
					in.readLong());
		}
		catch (RuntimeException ex)
		{
			this.moverIdx = new ScanIdx(0, tableSize);
		}
	
	}

	// TODO ��Ҫȫ��д������
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(this.formatterIdx.get());
		out.writeLong(this.formatterIdx.getFrom());
		out.writeLong(this.formatterIdx.getTo());
	
		out.writeLong(this.moverIdx.get());
		out.writeLong(this.moverIdx.getFrom());
		out.writeLong(this.moverIdx.getTo());
	}

	public long indexForHash(long hash)
	{
		return table.indexForHash(hash, this.tableSize);
	}

	public void add(Node node, long hash) throws IOException
	{
		assert (checkHashOfAddMethod(node, hash));
		table.add(indexForHash(hash), node);
	}

	private boolean checkHashOfAddMethod(Node node, long hash)
			throws IOException
	{
		long[] nodeHash = new long[1];
		boolean succ = node.getHash(nodeHash);
		return succ && nodeHash[0] == hash;
	}

	public ElementViewer keyToViewer(byte[] keyBuf, int keyBufOffset)
	{
		return table.keyToViewer(keyBuf, keyBufOffset);
	}

	public long keyToHash(ElementViewer keyViewer) throws IOException
	{
		return table.keyToHash(keyViewer);
	}

	public boolean keyEquals(ElementViewer keyViewerA, ElementViewer keyViewerB)
			throws IOException
	{
		return table.keyEquals(keyViewerA, keyViewerB);
	}

	public void lockSlot(long idx)
	{
		if (idx < 0 || idx >= tableSize)
		{
			throw new IndexOutOfBoundsException("idx illegal");
		}

		table.lockSlot(idx);
	}

	public void unlockSlot(long idx)
	{
		if (idx < 0 || idx >= tableSize)
		{
			throw new IndexOutOfBoundsException("idx illegal");
		}

		table.unlockSlot(idx);
	}

	public SlotIterator slotIterator(long slotIdx) throws IOException
	{
		if (slotIdx < 0 || slotIdx >= tableSize)
		{
			throw new IndexOutOfBoundsException("slotIdx illegal");
		}

		return table.slotIterator(slotIdx);
	}

}
