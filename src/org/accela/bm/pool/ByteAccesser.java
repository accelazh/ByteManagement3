package org.accela.bm.pool;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/*
 * ������ӿ����ײ㣬�ϲ�����ݽṹ�Ϳ���ѡ�������ļ��ϣ��ڴ������ϣ��ڴ��ļ�ӳ���ϣ�
 * �²���ϵͳswap�ļ������ϵȵȡ�
 */
public interface ByteAccesser extends Flushable, Closeable
{
	public void get(byte[] buf, int bufOffset, long idx, int length)
			throws IOException;

	public void set(byte[] buf, int bufOffset, long idx, int length)
			throws IOException;

	public long length() throws IOException;

	public void setLength(long newLength) throws IOException;
}
