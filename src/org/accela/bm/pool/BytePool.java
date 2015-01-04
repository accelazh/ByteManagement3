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
	// NOTE: ****NULL的值必须为-1，后面的程序依赖于它****
	public static final long NULL = -1;

	public ByteAccesser getAccesser();

	// 如果失败，则只有文件空间不足一种情况，抛出异常，而不是返回一个无效的key，即如果
	// 返回了一个key，则意味着操作成功
	public long alloc(long size) throws IOException;

	// key无效则返回false
	public boolean free(long key) throws IOException;

	// key无效则返回NULL
	public long realloc(long key, long size) throws IOException;

	public byte get(long key, long idx) throws IOException;

	// key not found错误通过返回值来通知，
	// 访问越界通过抛出BlockIndexOutOfBoundsException来通知，下同
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

	// 找不到key则返回null
	public byte[] fetch(long key) throws IOException;

	public <T> T fetch(long key, PersistanceDelegate<T> delegate)
			throws IOException, DataFormatException;

	public long replace(long key, byte[] data) throws IOException;

	public <T> long replace(long key, T object, PersistanceDelegate<T> delegate)
			throws IOException;

	public boolean remove(long key) throws IOException;
}
