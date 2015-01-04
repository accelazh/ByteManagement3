package org.accela.bm.collections.list;

import java.io.IOException;

import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementAccesser;

/*
��Ϊ�������Ҫ���ҵĵ�������java.lang.Iterator��java.util.ListIterator
����Ʒ������в�ͬ�����£�
1. java.util.Iterator��next()��������ǰ���ͷ���element���ҵĵ�������ǰ��forward
�ͺ���backward����������ǰ���ͺ��ˣ�������element��get����ר�ܷ��ص�ǰ��element��
�������ʹ�õ�������ͣ����һ��λ��ʱ�����Ե���get������η���element���������������
�ƶ�������Ҫ�ѵ�������һ��λ�ã��ƶ�����һ��λ��ʱ������forward��backward�������᷵��
element������ȡ����;��element�Ŀ����������ļ��У����������С����
2. Ϊ�˵�������ʹ�õļ�࣬�Լ��ܹ�ʹ��add����������β�����element���ҵĵ�����
û��hasNext��hasPrev����������һ��hasCur�����жϵ�ǰָ����Ƿ���һ���Ϸ���element��
���hasCur����true����ô����forward��backward����һ���Ϸ���������Ϊ��������ʹ��
��������index�ƶ���Χ����-1��list.size()��both inclusive������������λ��index=list.size()
ʱ������add�����������β���Ԫ�ء�
*/
public interface ByteListIterator extends ByteIterator<ElementAccesser>
{
	/*
	 * when this method returns true, it means the iterator currently points to
	 * an element. you can either invoke forward() of backward() at this time,
	 * both are legal. after that, the iterator may be moved to the next element
	 * or the previous element, or to the pos whose index==-1 or index==size().
	 * there are no elements on these positions, so hasCur() will return false
	 * then. in brief, the index range of the iterator is -1 ~ size() (both
	 * inclusive). when index==size(), you can invoke add() to add an element to
	 * the tail of the list.
	 */
	@Override
	public boolean hasCur() throws IOException;
	
	// move to the next element
	public void backward() throws IOException;
	
	// add an element to the pos that the iterator currently points to,
	// and shift the element previously at this pos and any subsequent elements
	// to the right. after add operation, the iterator points to the newly added
	// element.
	public void add(byte[] buf, int bufOffset, long idxInElement, int length)
			throws IOException;
	
	// return the index of the element the iterator currently points to
	public long index();
}
