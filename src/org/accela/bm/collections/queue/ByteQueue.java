package org.accela.bm.collections.queue;

import java.io.IOException;

import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementAccesser;
import org.accela.bm.collections.common.ElementContainer;
import org.accela.bm.common.Persistancer;

//˫ͷ���У�������Ϊ���л���ջ��ʹ�á�
public interface ByteQueue extends Persistancer, ElementContainer
{
	// TODO ��ϣ���Լ��Ժ�����ݽṹ����������ӿ�Clearable��������������
	// ���������queueʵ����Ӧ�������������
	public void addHead(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	public void addTail(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;

	public boolean removeHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public boolean removeTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public void get(long idx,
			byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public void set(long idx,
			byte[] bufNew,
			int bufOffsetNew,
			long idxInElementNew,
			int lengthNew,
			byte[] bufOld,
			int bufOffsetOld,
			long idxInElementOld,
			int lengthOld) throws IOException;

	public boolean getHead(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public boolean getTail(byte[] buf,
			int bufOffset,
			long idxInElement,
			int length) throws IOException;

	public ByteIterator<ElementAccesser> iterator(long idx) throws IOException;

	public ByteIterator<ElementAccesser> iterator() throws IOException;
}
