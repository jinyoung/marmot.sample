package basic;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleTransform {
	private static final String INPUT = "transit/subway_stations/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();

		Program program = Program.builder()
								.loadLayer(INPUT)
								.transform(b->b
									.outputRecordSchema("AREA:double,STR:string")
									.transformScript("AREA=ST_Area(the_geom);STR = KOR_SUB_NM+'['+OPE_MAN_ID+']'")
								)
								.storeAsCsv(RESULT)
								.build();

		catalog.deleteLayer(RESULT);
		marmot.execute("transform", program);
	}
}
