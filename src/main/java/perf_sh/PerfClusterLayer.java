package perf_sh;

import org.apache.log4j.PropertyConfigurator;

import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfClusterLayer {
	private static final String INPUT = "perf_sh/cadastral_part/heap";
	private static final String RESULT = "perf_sh/cadastral_part/clusters";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();

		catalog.deleteLayer(RESULT);
		marmot.clusterLayer(INPUT, RESULT);
	}
}
