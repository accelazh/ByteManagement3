package org.accela.bm.collections.common;

import java.io.IOException;

public interface ElementHasher
{
	// Note:
	// ElementEqualityComparator�ж���ȵ�Ԫ�أ�hashҲӦ����ȡ�
	// hash��ȵ�Ԫ�أ�ElementEqualityComparator���Բ���ȣ��������ȡ�
	// һ��Ԫ�صĹ�ϣ�룬Ӧ���������ֲ����
	public long hash(ElementViewer element) throws IOException;
}
