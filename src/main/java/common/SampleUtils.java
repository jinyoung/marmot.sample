package common;

import java.util.Map;

import com.google.common.collect.Maps;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUtils {
	public static void printPrefix(DataSet dataset, int count) {
		printPrefix(dataset.read(), count);
	}
	
	public static void printMarmotFilePrefix(MarmotRuntime marmot, String path, int count) {
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		printPrefix(marmot.readMarmotFile(path), count);
	}
	
	public static void printPrefix(RecordSet rset, int count) {
		RecordSchema schema = rset.getRecordSchema();
		Record record = DefaultRecord.of(schema);
		int[] colIdxs = schema.getColumnAll().stream()
							.filter(c -> !c.getType().isGeometryType())
							.mapToInt(c -> c.getOrdinal())
							.toArray();
		
		int i = 0;
		while ( ++i <= count && rset.next(record) ) {
			Map<String,Object> values = Maps.newHashMap();
			for ( int j =0; j < colIdxs.length; ++j ) {
				String name = schema.getColumnAt(colIdxs[j]).getName();
				Object value = record.get(colIdxs[j]);
				values.put(name, value);
			}
			System.out.println(values);
		}
	}
}
