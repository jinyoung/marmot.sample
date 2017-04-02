package basic;

import marmot.Record;
import marmot.RecordSet;
import marmot.remote.MarmotClient;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUtils {
	public static void printLayerPrefix(MarmotClient marmot, String layerName, int count) {
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(layerName);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		
		int i = 0;
		while ( ++i <= count && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
