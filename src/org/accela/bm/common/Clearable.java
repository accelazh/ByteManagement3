package org.accela.bm.common;

import java.io.IOException;

public interface Clearable
{
	// 与普通集合类不同，这个方法调用后，不光需要清空所有数据，
	// 还需要清空数据结构所占用的磁盘空间
	public void clear() throws IOException;
}
