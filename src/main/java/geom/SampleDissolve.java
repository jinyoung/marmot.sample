package geom;

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
public class SampleDissolve {
	private static final String RESULT = "tmp/sample/result";
	private static final String LAYER = "admin/cadastral_11/heap";
	private static final int NPARTS = 151;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		Program program = Program.builder()
								.loadLayer(LAYER)
								.transform("SD_SGG:string", "SD_SGG=INNB.substring(0,8)")
								.project("the_geom,SD_SGG")
								.dissolve("SD_SGG", NPARTS)
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("dissolve", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(RESULT);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		int count = 0;
		while ( ++count <= 10 && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
