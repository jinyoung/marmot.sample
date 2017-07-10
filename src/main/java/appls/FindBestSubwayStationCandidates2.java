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
public class FindBestSubwayStationCandidates2 {
	private static final String SID = "구역/시도";
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String STATIONS = "교통/지하철/역사";
	private static final String TEMP_STATIONS = "tmp/station_buffer";
	private static final String RESULT = "tmp/result";
	private static final DimensionDouble CELL_SIZE = new DimensionDouble(500, 500);
	
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
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		DataSet sid = marmot.getDataSet(SID);
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		Geometry seoul = marmot.executeLocally(plan)
								.toList()
								.get(0)
								.getGeometry(sid.getGeometryColumn());
		Envelope bounds = seoul.getEnvelopeInternal();
		
		DataSet taxi = marmot.getDataSet(TAXI_LOG);
		String geomCol = taxi.getGeometryColumn();
		String srid = taxi.getSRID();
		
		// 택시 운행 로그 기록에서 성울시 영역부분에서 승하차 로그 데이터만 추출한다.
		plan = marmot.planBuilder("find_subway_candiates")
					.load(TAXI_LOG)
					// 승차/하차 로그만 선택한다.
					.filter("status==1 || status==2")
					// 서울특별시 영역만의 로그만 선택한다.
					.intersects(taxi.getGeometryColumn(), seoul)
					// 모든 지하철 역사로부터 1km 이상 떨어진 로그 데이터만 선택한다.
					.spatialSemiJoin("the_geom", TEMP_STATIONS, INTERSECTS, true)
					// 각 로그 위치가 포함된 사각 셀을  부가한다.
					.assignSquareGridCell(geomCol, bounds, CELL_SIZE)
					.project("cell_geom as the_geom, cell_id, cell_pos")
					// 사각 그리드 셀 단위로 그룹핑하고, 각 그룹에 속한 레코드 수를 계산한다.
					.groupBy("cell_id")
						.taggedKeyColumns("the_geom,cell_pos")
						.aggregate(COUNT())
					// 'count' 컬럼 값을 기준으로 최대 10개의 그리드셀만을 선택한다.
					.pickTopK("count:D", 10)
					.project("the_geom,cell_pos,cell_id,count")
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		result = marmot.createDataSet(RESULT, "the_geom", srid, plan);
		
		marmot.deleteDataSet(TEMP_STATIONS);
		System.out.println("elapsed: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
