package org.accela.bm.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//如果你不喜欢为T这个类另外编写一个PersistanceDelegate，而希望它自己
//负责自己的持久化，那么你只需要让T实现PersistanceDelegate接口即可
public interface PersistanceDelegate<T>
{
	// NOTE：如果你在使用in读入的数据，生成对象时，发现数据不合法，比如读出来的数组
	// 长度为负数，无法新建数组等，此时就需要抛出DataFormatException。使用IO存取的
	// 数据是可能损坏的，DataFormatException就是用来表明数据损坏的异常。
	public T read(DataInput in) throws IOException, DataFormatException;

	public void write(DataOutput out, T object) throws IOException;

}
