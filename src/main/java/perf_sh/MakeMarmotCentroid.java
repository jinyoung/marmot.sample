package perf_sh;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MakeMarmotCentroid {
	private static final String INPUT = "perf_sh/cadastral_part/heap";
	private static final String RESULT = "perf_sh/cadstral_center/heap";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		String schemaStr = "the_geom:point";
		String trans = "the_geom = ST_Centroid(the_geom)";

		Program program = Program.builder()
								.loadLayer(INPUT)
								.transform(schemaStr, trans, true)
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute("make_marmot_centroid", program);
	}
}
