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
public class PerfConvexHull {
	private static final String INPUT = "perf_sh/cadstral_center/heap";
	private static final String RESULT = "tmp/perf_sh/mbr";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadLayer(INPUT)
								.aggregate(AggregateFunction.ConvexHull())
								.store(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute("perf_convex_hull", program);
	}
}

