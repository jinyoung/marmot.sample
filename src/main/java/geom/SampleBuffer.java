package geom;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleBuffer {
	private static final String INPUT = "transit/subway_stations/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(INPUT)
								.transform("the_geom = ST_Buffer(the_geom, 50)")
//								.buffer("the_geom", 50)
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("buffer", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
