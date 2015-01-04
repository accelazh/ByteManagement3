package org.accela.bm.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * �μ�BinaryBytePool�����ʹ������ӿڵġ�
 * ʵ������ӿڵĶ����Լ������Լ�������״̬�ĳ־û���
 * write���ڷ�����������������״̬д��־û����ʡ���
 * ������ù��캯����������󣬱������create��������
 * read����֮һ��create������ʾ����������½��ģ�����
 * ��Ӧ�ĳ�ʼ����read������ʾ�������ʹ�ó־û�������
 * �������Ϣ�ָ�����ʱ��״̬����Ҫ�ӳ־û������ж�ȡ���ݣ�
 * ������Ӧ��ʼ����
 * ��create/read����������Ϻ������������ʼ����ϡ���
 * ����������빤��״̬�����Ե��ó��淽��������Ҫ�־û����
 * �����ʱ�򣬵���write�����������״̬��Ϣ�������ڳ־û�
 * �����С�
 */
public interface Persistancer
{
	public void create(Object... args) throws IOException;

	// read/write����Ӧ�ñ�֤���Լ�д���˶������ݣ����ܹ����������ݣ�����������ٶ���
	// ����Ӱ�������˼��������������
	public void read(DataInput in) throws IOException, DataFormatException;

	public void write(DataOutput out) throws IOException;
}
