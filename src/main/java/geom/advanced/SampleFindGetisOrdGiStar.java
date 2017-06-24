package geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.geo.LISAWeight;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFindGetisOrdGiStar {
	private static final String RESULT = "tmp/result";
	private static final String INPUT = "시연/대전공장";
	private static final String VALUE_COLUMN = "FCTR_MEAS";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Plan plan = marmot.planBuilder("local_spatial_auto_correlation")
								.loadGetisOrdGi(INPUT, VALUE_COLUMN, 1000,
												LISAWeight.FIXED_DISTANCE_BAND)
								.project("UID,gi_zscore,gi_pvalue")
								.sort("UID")
								.storeAsCsv(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
//		RecordSet rset = marmot.readLayer(RESULT);
//		Record record = DefaultRecord.of(rset.getRecordSchema());
//		int nrecords = 0;
//		while ( ++nrecords <= 10 && rset.next(record) ) {
//			System.out.println(record);
//		}
	}
}
