package perf_sh;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.optor.geo.SpatialRelationship;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfSpatialJoinIdx2 {
	private static final String OUTER = "transit/subway_stations/clusters";
	private static final String INNER = "perf_sh/cadastral_part/clusters";
	private static final String RESULT = "tmp/perf_sh/joined2";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.loadSpatialIndexJoin(SpatialRelationship.INTERSECTS,
													OUTER, INNER, "*",
													"the_geom as the_geom2,*-{the_geom}")
								.store(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute("perf_spatial_join", program);
	}
}
