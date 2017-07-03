package appls;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

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
public class FindBestSubwayStationCandidates {
	private static final String SID = "구역/시도";
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String TEMP_SEOUL_TAXI = "tmp/taxi_seoul";
	private static final String TEMP_STATIONS = "tmp/station_buffer";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
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
		
		DataSet input = marmot.getDataSet(SID);
		String geomCol = input.getGeometryColumn();
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		Geometry seoul = marmot.executeLocally(plan).toList().get(0).getGeometry(geomCol);
		Envelope bounds = seoul.getEnvelopeInternal();

		// 서울지역 지하철 역사를 구하고 1km 버퍼를 구한다.
		DataSet stations = marmot.getDataSet(STATIONS);
		String statGeomCol = stations.getGeometryColumn();
		plan = marmot.planBuilder("")
					.load(STATIONS)
					.filter("sig_cd.substring(0,2) == '11'")
					.buffer(statGeomCol, statGeomCol, 1000)
					.store(TEMP_STATIONS)
					.build();
		marmot.deleteDataSet(TEMP_STATIONS);
		result = marmot.createDataSet(TEMP_STATIONS, statGeomCol, stations.getSRID(), plan);
		
		// 택시 운행 로그 기록에서 서울시 영역에서 지하철 역사에서 1km 떨어진 승하차 로그 데이터만 추출한다.
		DataSet taxi = marmot.getDataSet(TAXI_LOG);
		String taxiGeomCol = taxi.getGeometryColumn();
		plan = marmot.planBuilder("seoul_taxi")
					.load(TAXI_LOG)
					// 승하차 로그 데이터만 추출한다.
					.filter("status==1 || status==2")
					// 서울시 영역만 추출한다.
					.intersects(taxiGeomCol, seoul)
					// 지하철 역사에서 1km 떨어진 영역만 추출한다.
					.differenceJoin("the_geom", TEMP_STATIONS)
					.store(TEMP_SEOUL_TAXI)
					.build();
		marmot.deleteDataSet(TEMP_SEOUL_TAXI);
		result = marmot.createDataSet(TEMP_SEOUL_TAXI, taxiGeomCol, taxi.getSRID(), plan);
		result.cluster();
		
		// 서울특별시를 커버하는 500mx500m 사각 그리드를 생성하고, 각 셀에 포함된
		// 승하차 지점의 갯수를 센다.
		plan = marmot.planBuilder("grid_seould_500x500")
					.loadSquareGridFile(bounds, new DimensionDouble(500, 500))
					.aggregateJoin("the_geom", TEMP_SEOUL_TAXI, INTERSECTS, COUNT())
					// 'count' 컬럼 값을 기준으로 최대 10개의 그리드셀만을 선택한다.
					.pickTopK("count:D", 10)
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		result = marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", plan);
		
		marmot.deleteDataSet(TEMP_STATIONS);
		marmot.deleteDataSet(TEMP_SEOUL_TAXI);
		
		System.out.println("elapsed: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
