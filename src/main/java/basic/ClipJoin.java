package basic;

import org.apache.log4j.PropertyConfigurator;

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
public class ClipJoin {
	private static final String RESULT = "tmp/sample/result";
	private static final String OUTER = "admin/cadastral/heap";
	private static final String INNER = "admin/urban_area";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		Program program = Program.builder()
								.loadLayer(OUTER)
								.clipJoin("the_geom", INNER)
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		catalog.deleteLayer(RESULT);
		marmot.execute("clip_join", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(RESULT);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		int count = 0;
		while ( ++count <= 10 && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
