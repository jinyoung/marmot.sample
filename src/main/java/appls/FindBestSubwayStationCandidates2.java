package appls;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
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
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
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
		plan = marmot.planBuilder("find_best_spots")
					.load(TAXI_LOG)
					// 승차/하차 로그만 선택한다.
					.filter("status==1 || status==2")
					// 서울영역만의 로그만 선택한다.
					.intersects(taxi.getGeometryColumn(), seoul)
					// 포함된 사각 셀을  부가한다.
					.assignSquareGridCell(geomCol, bounds, new DimensionDouble(500, 500))
					.project("cell_geom as the_geom, cell_id, cell_pos")
					// 사각 그리드 셀 단위로 그룹핑하고, 각 그룹에 속한 레코드 수를 계산한다.
					.groupBy("cell_id")
						.taggedKeyColumns("the_geom,cell_pos")
						.aggregate(COUNT())
					// 1km 지하철 역사 버버와 겹치는 영역을 제거한다.
					.differenceJoin("the_geom", TEMP_STATIONS)
					// 일부 겹친 경우, 남은 영역이 90000보다 작은 경우는 제외시킨다.
					.filter("ST_Area(the_geom) > 120000")
					// 'count' 컬럼 값을 기준으로 최대 10개의 그리드셀만을 선택한다.
					.sort("count:D")
					.project("the_geom,cell_pos,cell_id,count")
					.store(RESULT)
					.build();
		marmot.deleteDataSet(RESULT);
		result = marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", plan);
		
		marmot.deleteDataSet(TEMP_STATIONS);
		
		System.out.println("elapsed: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 5);
	}
}
