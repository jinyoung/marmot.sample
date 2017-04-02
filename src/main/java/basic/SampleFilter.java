package basic;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFilter {
	private static final String INPUT = "transit/subway_stations/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(INPUT)
								.filter("KOR_SUB_NM.length() == 6")
								.project("KOR_SUB_NM")
								.storeAsCsv(RESULT)
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("transform", program);
		
//		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
