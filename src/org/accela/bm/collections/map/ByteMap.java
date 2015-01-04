package org.accela.bm.collections.map;

import java.io.IOException;

import org.accela.bm.collections.common.ByteIterator;
import org.accela.bm.collections.common.ElementFilter;
import org.accela.bm.common.Concurrentable;
import org.accela.bm.common.Persistancer;

public interface ByteMap extends Persistancer, MapContainer, Concurrentable
{
	// TODO ע��key��hash�͵ȼ��ԣ���ͬ��key��byte�ֽ����飬hash�͵ȼ����ܷ�ά�֣�

	// ����ֵΪtrue��ʾ��ȥû����key��Ӧ��value���ʶ������ؾ�value
	// keyLengthΪMapContainer.getKeyLength()
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

	// ����ֵΪtrue��ʾ�ɹ��������µ�key-value�ԣ�����ȥû����key������value��
	// ���Ҳ�����ؾɵ�Value
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

	// ����ֵΪtrue��ʾ��ȥ������key��Ӧ��value
	public boolean get(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBuf,
			int valueBufOffset,
			long valueIdxInElement,
			int valueLength) throws IOException;

	// ����true��ʾ��ȥ������key��Ӧ��value
	public boolean remove(byte[] keyBuf,
			int keyBufOffset,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	// ���ҽ���key��filterָ����value����ʱ���Ż�ɾ����������true
	public boolean removeIf(byte[] keyBuf,
			int keyBufOffset,
			ElementFilter oldValueFilter,
			byte[] valueBufOld,
			int valueBufOffsetOld,
			long valueIdxInElementOld,
			int valueLengthOld) throws IOException;

	// ���ҽ�����ȥ�Ѿ�����key��ĳ��ֵ�������ŻὫ���ֵ�滻���µ�valueNew��
	// �����ؾ��е�valueOld
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

	// ���ҽ�����ȥ��key��ĳ��ֵ���������ҹ�����ֵ����oldValueFilter���ŻὫ��
	// �滻Ϊ�������valueNew�������ؾɵ�valueOld
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
