package basic;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleFilter {
	private static final String INPUT = "transit/subway/stations/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(INPUT)
								.filter("kor_sub_nm.length() == 6")
								.project("kor_sub_nm as kor_nm")
								.store(RESULT)
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("filter", program);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
