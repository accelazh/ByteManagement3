package org.accela.bm.collections.map;

import java.io.IOException;

import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementFilter;
import org.accela.bm.common.Concurrentable;
import org.accela.bm.common.Persistancer;

public interface ByteMap extends Persistancer, MapContainer, Concurrentable
{
	// TODO 注意key的hash和等价性，不同的key是byte字节数组，hash和等价性能否维持？

	// 返回值为true表示过去没有与key对应的value，故而不返回旧value
	// keyLength为MapContainer.getKeyLength()
	public boolean put(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBufNew,
			int valueBufOffsetNew,
			long valueIdxInElementNew,
			int valueLengthNew,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	// 返回值为true表示成功放入了新的key-value对，即过去没有与key关联的value，
	// 因而也不返回旧的Value
	public boolean putIfAbsent(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBufNew,
			int valueBufOffsetNew,
			long valueIdxInElementNew,
			int valueLengthNew,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	// 返回值为true表示过去存在与key对应的value
	public boolean get(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBuf,
			int valueBufOffset,
			long valueIdxInElement,
			int valueLength) throws IOException;

	// 返回true表示过去存在与key对应的value
	public boolean remove(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	// 当且仅当key与filter指定的value关联时，才会删除，并返回true
	public boolean removeIf(byte[] keyBuf,
			int keyBufOffset,
			ElementFilter oldValueFilter,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	// 当且仅当过去已经存在key与某个值关联，才会将这个值替换成新的valueNew，
	// 并返回旧有的valueOld
	public boolean replace(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBufNew,
			int valueBufOffsetNew,
			long valueIdxInElementNew,
			int valueLengthNew,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	// 当且仅当过去的key与某个值关联，并且关联的值满足oldValueFilter，才会将其
	// 替换为传入的新valueNew，并返回旧的valueOld
	public boolean replaceIf(byte[] keyBuf,
			int keyBufOffset,
			ElementFilter oldValueFilter,
			byte[] valueBufNew,
			int valueBufOffsetNew,
			long valueIdxInElementNew,
			int valueLengthNew,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	public ByteIterator<EntryAccesser> iterator() throws IOException;

	public boolean containsKey(byte[] keyBuf, int keyBufOffset)
			throws IOException;

}
