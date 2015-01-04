package org.accela.bm.pool;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/*
 * 用这个接口作底层，上层的数据结构就可以选择工作在文件上，内存数组上，内存文件映射上，
 * 仿操作系统swap文件机制上等等。
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
