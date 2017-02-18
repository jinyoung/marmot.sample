package carloc;

import marmot.Program;
import marmot.Record;
import marmot.RecordSet;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportTaxiTripLogs {
	private static final String INPUT = "data/carloc_hst";
	private static final String RESULT = "taxi/trip/heap";
	
	public static final void main(String... args) throws Exception {
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		Program program = Program.builder()
								.loadTextFile(INPUT)
								.transformWithResourceScript("classpath:carloc/ParseTaxiTripLog.xml")
								.transformCRS("the_geom", "EPSG:4326", "EPSG:5186")
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		catalog.deleteLayer(RESULT);
		marmot.execute("import_taxi_trip_logs", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(RESULT);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		int count = 0;
		while ( ++count <= 10 && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
