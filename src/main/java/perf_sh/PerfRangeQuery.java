package perf_sh;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;

import marmot.Program;
import marmot.geo.GeoClientUtils;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PerfRangeQuery {
	private static final String INPUT = "perf_sh/cadastral_part/heap";
//	private static final String INPUT = "perf_sh/cadastral_part/clusters";
	private static final String RESULT = "tmp/perf_sh/range";
	
	private static final Coordinate[][] RANGES = new Coordinate[][]{
		new Coordinate[]{new Coordinate(127.02467553370682651,37.49991173322327143),
						new Coordinate(127.03202455333786247,37.49313743202948501)},
		new Coordinate[]{new Coordinate(126.97438240688870792,37.5692093431122629),
						new Coordinate(126.98706956845671812,37.56225119625899822)},
		new Coordinate[]{new Coordinate(127.00122216156178467,37.28067358969770595),
						new Coordinate(127.01816859616074851,37.26592092910115639)},
		new Coordinate[]{new Coordinate(126.670243,37.191270),
						new Coordinate(127.658326,37.498328)},
	};
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		if ( args.length < 1 ) {
			System.err.printf("target key number (0~3) is not provided%n");
			System.exit(-1);
		}
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		int idx = Integer.parseInt(args[0]);
		Coordinate tl = RANGES[idx][0];
		Coordinate br = RANGES[idx][1];
		Polygon key = GeoClientUtils.toPolygon(GeoClientUtils.toEnvelope(tl, br));

		Program program = Program.builder()
								.loadLayer(INPUT, "intersects", key)
								.store(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute("perf_mbr", program);
	}
}
