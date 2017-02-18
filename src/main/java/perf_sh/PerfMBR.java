package perf_sh;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.optor.geo.AggregateFunction;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfMBR {
	private static final String INPUT = "perf_sh/cadastral_part/heap";
	private static final String RESULT = "tmp/perf_sh/mbr";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(INPUT)
								.aggregate(AggregateFunction.MBR())
								.storeAsCsv(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute("perf_mbr", program);
	}
}
