package geom.advanced;

import java.util.Map;
import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Maps;

import marmot.Program;
import marmot.Record;
import marmot.optor.geo.AggregateFunction;
import marmot.optor.geo.LISAWeight;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFindLocalMoranI {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "tmp/tc9/heap";
	private static final String VALUE_COLUMN = "FCTR_MEAS";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		String tempPath = "tmp/" + UUID.randomUUID();
		Program program0 = Program.builder()
								.loadLayer(INPUT)
								.aggregate(AggregateFunction.COUNT(),
											AggregateFunction.AVG(VALUE_COLUMN),
											AggregateFunction.STDDEV(VALUE_COLUMN))
								.store(tempPath)
								.build();
		marmot.execute("find_statistics", program0);
		
		Map<String,Object> params = Maps.newHashMap();

		Record result = marmot.readMarmotFile(tempPath).stream().findAny().get();
		params.putAll(result.toMap());
		marmot.deleteFile(tempPath);
		
		double avg = (Double)params.get("avg");
		Program program1 = Program.builder()
								.loadLayer(INPUT)
								.transform("diff:double", "diff = FCTR_MEAS -" + avg)
								.transform("diff2:double,diff4:double",
											"diff2 = diff * diff; diff4=diff2*diff2")
								.aggregate(AggregateFunction.SUM("diff").as("diffSum"),
											AggregateFunction.SUM("diff2").as("diff2Sum"),
											AggregateFunction.SUM("diff4").as("diff4Sum"))
								.store(tempPath)
								.build();
		marmot.execute("find_statistics2", program1);
		Record result2 = marmot.readMarmotFile(tempPath).stream().findAny().get();
		params.putAll(result2.toMap());
		marmot.deleteFile(tempPath);
		
		Program program = Program.builder()
								.loadLocalMoranI(INPUT, "UID", "FCTR_MEAS", 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("UID,moran_i,moran_zscore,moran_pvalue")
								.sort("UID")
								.storeAsCsv(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute("local_spatial_auto_correlation", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
//		RecordSet rset = marmot.readLayer(RESULT);
//		Record record = DefaultRecord.of(rset.getRecordSchema());
//		int nrecords = 0;
//		while ( ++nrecords <= 10 && rset.next(record) ) {
//			System.out.println(record);
//		}
	}
}