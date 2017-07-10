package yunsei;

import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.DimensionDouble;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Y2T_2 {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String SID = "구역/시도";
	private static final String TEMP_TAXI = "tmp/taxi";
	private static final String RESULT = "tmp/result";
	
	private static final DimensionDouble CELL_SIZE = new DimensionDouble(1000,1000);
	private static final int NWORKERS = 25;
	
	public static final void main(String... args) throws Exception {
//		PropertyConfigurator.configure("log4j.properties");
		LogManager.getRootLogger().setLevel(Level.OFF);
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = cl.getOptionValue("host", "localhost");
		int port = cl.getOptionInt("port", 12985);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		Plan plan;
		DataSet result;
		
		DataSet input = marmot.getDataSet(TAXI_LOG);
		String geomCol = input.getGeometryColumn();
		String srid = input.getSRID();
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출하고, 영역의 MBR를 구한다.
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		Geometry seoul = marmot.executeLocally(plan).toList().get(0).getGeometry(geomCol);
		Envelope bounds = seoul.getEnvelopeInternal();
		
		plan = marmot.planBuilder("filter_taxi_logs")
					.load(TAXI_LOG)
					.filter("status == 1 || status == 2")
					.store(TEMP_TAXI)
					.build();
		marmot.deleteDataSet(TEMP_TAXI);
		result = marmot.createDataSet(TEMP_TAXI, geomCol, srid, plan);
		System.out.println("done: filter taxi_logs, elapsed=" + watch.getElapsedTimeString());
		result.cluster();
		System.out.println("done: index taxi_logs, elapsed=" + watch.getElapsedTimeString());
		
		String expr = "if ( status == null ) { supply = 0; demand = 0; }" 
					+ "else if ( status == 2 ) { supply = 1; demand = 0; }"
					+ "else if ( status == 1 ) { supply = 0; demand = 1; }";

		// 버스 승하차 정보에서 서울 구역부분만 추출한다.
		plan = marmot.planBuilder("build_square_grid")
					.loadSquareGridFile(bounds, CELL_SIZE, NWORKERS)
					.spatialOuterJoin("the_geom", TEMP_TAXI, INTERSECTS, "*,param.status")
					.update("supply:int,demand:int", expr)
					.groupBy("cell_id")
						.taggedKeyColumns("the_geom")
						.aggregate(SUM("supply").as("supply_count"),
									SUM("demand").as("demand_count"))
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		result = marmot.createDataSet(RESULT, geomCol, srid, plan);	
		System.out.println("done, elapsed=" + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
