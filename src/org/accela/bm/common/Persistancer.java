package org.accela.bm.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * 参见BinaryBytePool是如何使用这个接口的。
 * 实现这个接口的对象将自己管理自己的运行状态的持久化。
 * write用于方法将这个对象的运行状态写入持久化介质。当
 * 这个调用构造函数创建对象后，必须调用create方法或者
 * read方法之一。create方法表示这个对象是新建的，将作
 * 相应的初始化。read方法表示这个对象将使用持久化介质中
 * 保存的信息恢复保存时的状态，需要从持久化介质中读取数据，
 * 并作相应初始化。
 * 在create/read方法调用完毕后，这个对象才算初始化完毕。接
 * 下来对象进入工作状态，可以调用常规方法。当需要持久化这个
 * 对象的时候，调用write方法输出对象状态信息，保存在持久化
 * 介质中。
 */
public interface Persistancer
{
	public void create(Object... args) throws IOException;

	// read/write方法应该保证，自己写出了多少数据，就能够读多少数据，不多读或者少读，
	// 以免影响其它人继续读后面的数据
	public void read(DataInput in) throws IOException, DataFormatException;

	public void write(DataOutput out) throws IOException;
}
