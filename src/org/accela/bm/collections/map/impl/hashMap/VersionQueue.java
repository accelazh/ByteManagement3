package org.accela.bm.collections.map.impl.hashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.accela.bm.common.Common;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.common.Persistancer;

//�����汾�¾ɸ��棬����˶��еĽṹ��������Ϊ��ϣ��ֻ��Ҫ�������µ������汾��
//��˰汾���г���һ���˻�Ϊ2��Ԫ�صĶ���
public class VersionQueue implements Persistancer
{
	private RawTable rawTable = null;

	private VersionedTable cur = null;

	private VersionedTable last = null;

	public VersionQueue(RawTable rawTable)
	{
		if (null == rawTable)
		{
			throw new NullPointerException("rawTable is null");
		}

		this.rawTable = rawTable;
	}

	public RawTable getRawTable()
	{
		return rawTable;
	}

	public VersionedTable getCur()
	{
		return cur;
	}

	public VersionedTable getLast()
	{
		return last;
	}

	// TODO ��Ҫȫ��д��������
	public void enqueue(VersionedTable newVersion)
	{
		if (null == newVersion
				|| newVersion.tableSize() <= cur.tableSize()
				|| !newVersion.formatterIdx().isFinished())
		{
			throw new IllegalArgumentException("illegal newVersion");
		}

		this.last = this.cur;
		this.cur = newVersion;
	}

	@Override
	public void create(Object... args) throws IOException
	{
		VersionedTable last = (VersionedTable) args[0];
		VersionedTable cur = (VersionedTable) args[1];
		if (null == last || null == cur)
		{
			throw new NullPointerException("null last or null cur");
		}
		if (!last.formatterIdx().isFinished()
				|| !cur.formatterIdx().isFinished())
		{
			throw new NullPointerException("illegal last or illegal cur");
		}
		if (cur.tableSize() <= last.tableSize())
		{
			throw new NullPointerException("illegal cur");
		}

		this.last = last;
		this.cur = cur;
	}

	@Override
	public void read(DataInput in) throws IOException, DataFormatException
	{
		long lastTableSize = in.readLong();
		last = new VersionedTable(rawTable, lastTableSize);
		last.read(in);

		long curTableSize = in.readLong();
		cur = new VersionedTable(rawTable, curTableSize);
		cur.read(in);
	}

	// TODO ��Ҫȫ��д��������
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(last.tableSize());
		last.write(out);
		out.writeLong(cur.tableSize());
		cur.write(out);
	}
}
