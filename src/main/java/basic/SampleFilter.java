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
	private static final String INPUT = "POI/주유소_가격";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder("filter")
								.load(INPUT)
								.filter("휘발유 > 2000")
								.project("상호,휘발유")
								.storeMarmotFile(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute(program);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
