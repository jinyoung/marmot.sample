package carloc;

import java.time.LocalDateTime;
import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import utils.StopWatch;
import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.support.DateTimeFunctions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindPassingStation {
	private static final String INPUT = "taxi/taxi.trj";
	private static final String OUTPUT = "result";
	private static final LocalDateTime START = LocalDateTime.of(2016, 1, 27, 8, 0);
	private static final LocalDateTime END = LocalDateTime.of(2016, 1, 27, 10, 0);

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		StopWatch watch = StopWatch.start();

		String init1 = String.format("$key=ST_ITFromDateTime(ST_DTFromString('%s'), "
															+ "ST_DTFromString('%s'));",
										DateTimeFunctions.ST_DTToString(START),
										DateTimeFunctions.ST_DTToString(END));
		String pred1 = "ST_ITOverlaps(drive_interval,$key)";
		
		Geometry key = getSubwayStations(marmot, "사당역");
		String schema = "the_geom:line_string";
		String tran = "the_geom = ST_TRLineString(trajectory)";
		
		Program program = Program.builder()
								.loadMarmotFile(INPUT)
								.filter("status == 3")
								.filter(init1, pred1)
								.transform("the_geom:line_string",
											"the_geom = ST_TRLineString(trajectory)")
								.withinDistance("the_geom", key, 100)
								.project("*-{trajectory}")
								.storeLayer(OUTPUT, "the_geom", "EPSG:5186")
								.build();
		
		marmot.execute("find_passing_station", program);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}

	private static final String SUBWAY_STATIONS_LYAER = "transit/subway_stations/heap";
	private static Geometry getSubwayStations(MarmotClient marmot, String stationName)
		throws Exception {
		String predicate = String.format("KOR_SUB_NM == '%s'", stationName);
		Program program = Program.builder()
								.loadLayer(SUBWAY_STATIONS_LYAER)
								.filter(predicate)
								.project("the_geom")
								.build();
		return marmot.executeAndGetResult(program)
						.stream()
						.map(rec -> rec.getGeometry(0))
						.findAny()
						.orElse(null);
	}
}
