package org.accela.bm.collections.common;

import java.io.IOException;

public interface ElementAccesser extends ElementViewer
{
	public void set(byte[] bufNew,
			int bufOffsetNew,
			long idxInElementNew,
			int lengthNew,
			byte[] bufOld,
			int bufOffsetOld,
			long idxInElementOld,
			int lengthOld) throws IOException;
}
