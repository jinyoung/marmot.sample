package carloc;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindPassingStation {
	private static final String TAXI_TRJ = "로그/나비콜/택시경로";
	private static final String OUTPUT = "tmp/result";
	private static final String SRID = "EPSG:5186";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		StopWatch watch = StopWatch.start();
		
		Geometry key = getSubwayStations(marmot, "사당역");
		Program program = Program.builder("find_passing_station")
								.load(TAXI_TRJ)
								.filter("status == 3")
								.update("the_geom:line_string",
											"the_geom = ST_TRLineString(trajectory)")
								.withinDistance("the_geom", key, 100)
								.project("*-{trajectory}")
								.store(OUTPUT)
								.build();
		
		marmot.deleteDataSet(OUTPUT);
		DataSet result = marmot.createDataSet(OUTPUT, "the_geom", SRID, program);
		
		SampleUtils.printPrefix(result, 5);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}

	private static final String SUBWAY_STATIONS = "교통/지하철/서울역사";
	private static Geometry getSubwayStations(MarmotClient marmot, String stationName)
		throws Exception {
		String predicate = String.format("kor_sub_nm == '%s'", stationName);
		Program program = Program.builder()
								.load(SUBWAY_STATIONS)
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
