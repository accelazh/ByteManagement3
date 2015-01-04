package org.accela.bm.pool.test;

import java.util.ArrayList;
import java.util.List;

public class SectionList
{
	private List<Section> list = new ArrayList<Section>();

	public SectionList()
	{
		// do nothing
	}

	public synchronized int size()
	{
		return list.size();
	}

	public synchronized int findInsertion(Section section)
	{
		int insertion = 0;
		for (insertion = 0; insertion < list.size(); insertion++)
		{
			if (list.get(insertion).getPos() >= section.getPos())
			{
				break;
			}
		}

		Section right = insertion < list.size() ? list.get(insertion) : null;
		Section left = insertion - 1 >= 0 ? list.get(insertion - 1) : null;
		if ((null == right || (right.getPos() > section.getPos() && section
				.getPos() + section.getLength() <= right.getPos()))
				&& (null == left || (left.getPos() < section.getPos() && left
						.getPos() + left.getLength() <= section.getPos())))
		{
			return insertion;
		}
		else
		{
			System.out.println(Thread.currentThread()
					+ "SectionList.findInsertion() failed: insertion="
					+ insertion
					+ ", list.size(): "
					+ list.size()
					+ ", left="
					+ left
					+ ", section="
					+ section
					+ ", right="
					+ right);
			assert (false);
			return -1;
		}
	}

	public synchronized boolean canInsert(Section section)
	{
		return findInsertion(section) >= 0;
	}

	public synchronized boolean add(Section section)
	{
		int insertion = findInsertion(section);
		if (insertion >= 0)
		{
			list.add(insertion, section);
			return true;
		}
		else
		{
			return false;
		}

	}

	public synchronized Section get(int idx)
	{
		return list.get(idx);
	}

	public synchronized Section remove(int idx)
	{
		return list.remove(idx);
	}

	public synchronized int find(long pos)
	{
		for (int i = 0; i < list.size(); i++)
		{
			if (list.get(i).getPos() == pos)
			{
				return i;
			}
		}
		return -1;
	}

	public synchronized boolean remove(long pos)
	{
		int idx = find(pos);
		if (idx < 0)
		{
			return false;
		}
		remove(idx);
		return true;
	}

	public synchronized void clear()
	{
		list.clear();
	}

	public static class Section
	{
		private long pos = 0;
		private long length = 0;

		public Section(long pos, long length)
		{
			if (pos < 0 || length < 0)
			{
				throw new IllegalArgumentException();
			}

			this.pos = pos;
			this.length = length;
		}

		public long getPos()
		{
			return pos;
		}

		public long getLength()
		{
			return length;
		}

		@Override
		public String toString()
		{
			return "Section [pos=" + pos + ", length=" + length + "]";
		}

	}
}
