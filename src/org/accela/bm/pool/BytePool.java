package org.accela.bm.pool;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.accela.bm.common.Concurrentable;
import org.accela.bm.common.DataFormatException;
import org.accela.bm.common.PersistanceDelegate;
import org.accela.bm.common.Persistancer;
import org.accela.bm.common.PoolSizable;

public interface BytePool extends Flushable, Closeable, PoolSizable,
		Persistancer, Concurrentable
{
	// NOTE: ****NULL��ֵ����Ϊ-1������ĳ�����������****
	public static final long NULL = -1;

	public ByteAccesser getAccesser();

	// ���ʧ�ܣ���ֻ���ļ��ռ䲻��һ��������׳��쳣�������Ƿ���һ����Ч��key�������
	// ������һ��key������ζ�Ų����ɹ�
	public long alloc(long size) throws IOException;

	// key��Ч�򷵻�false
	public boolean free(long key) throws IOException;

	// key��Ч�򷵻�NULL
	public long realloc(long key, long size) throws IOException;

	public byte get(long key, long idx) throws IOException;

	// key not found����ͨ������ֵ��֪ͨ��
	// ����Խ��ͨ���׳�BlockIndexOutOfBoundsException��֪ͨ����ͬ
	public boolean set(long key, long idx, byte value) throws IOException;

	public boolean get(long key, byte[] buf, long idx) throws IOException;

	public boolean set(long key, byte[] buf, long idx) throws IOException;

	public boolean get(long key, byte[] buf, int bufOffset, long idx, int length)
			throws IOException;

	public boolean set(long key, byte[] buf, int bufOffset, long idx, int length)
			throws IOException;

	// =====================================================================

	public long put(byte[] data) throws IOException;

	public <T> long put(T object, PersistanceDelegate<T> delegate)
			throws IOException;

	// �Ҳ���key�򷵻�null
	public byte[] fetch(long key) throws IOException;

	public <T> T fetch(long key, PersistanceDelegate<T> delegate)
			throws IOException, DataFormatException;

	public long replace(long key, byte[] data) throws IOException;

	public <T> long replace(long key, T object, PersistanceDelegate<T> delegate)
			throws IOException;

	public boolean remove(long key) throws IOException;
}
