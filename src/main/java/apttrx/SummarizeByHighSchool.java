package apttrx;

import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SummarizeByHighSchool {
	private static final String APT_TRX = "주택/실거래/아파트매매";
	private static final String APT_LOC = "tmp/아파트실매매/아파트위치";
	private static final String SCHOOLS = "POI/전국초중등학교";
	private static final String HIGH_SCHOOLS = "tmp/아파트실매매/고등학교";
//	private static final String RESULT = "tmp/아파트실매매/고등학교주변";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		StopWatch watch = StopWatch.start();

		Plan plan;
		
		//전국초중등학교 정보에서 고등학교만 뽑는다.
		DataSet highSchools = marmot.getDataSet(HIGH_SCHOOLS, null);
		if ( highSchools == null ) {
			DataSet ds = marmot.getDataSet(SCHOOLS);
			String geomCol = ds.getGeometryColumn();
			String srid = ds.getSRID();
		
			plan = marmot.planBuilder("find_high_school")
						.load(SCHOOLS)
						.filter("type == '고등학교'")
						.store(HIGH_SCHOOLS)
						.build();
			highSchools = marmot.createDataSet(HIGH_SCHOOLS, geomCol, srid, plan);
		}
		
		String geomCol = highSchools.getGeometryColumn();
		String srid = highSchools.getSRID();
		
		plan = marmot.planBuilder("summarize_by_school")
						.load(APT_LOC)
						
						// 고등학교 주변 1km 내의 아파트 검색.
						.centroid("the_geom", "the_geom")
						.buffer("the_geom", "circle", 1000)
						.spatialJoin("circle", HIGH_SCHOOLS, SpatialRelation.INTERSECTS,
									String.format("*-{the_geom},param.{%s,id,name}",geomCol))
						
						// 고등학교 1km내 위치에 해당하는 아파트 거래 정보를 검색.
						.join("시군구,번지,단지명", APT_TRX, "시군구,번지,단지명",
								"the_geom,id,name,param.*", null)
						// 평당 거래액 계산.
						.update("평당거래액:int",
								"평당거래액 = (int)Math.round((거래금액*3.3) / 전용면적);")
						
						// 고등학교를 기준으로 그룹핑하여 집계한다.
						.groupBy("id")
						.taggedKeyColumns("the_geom,name")
						.aggregate(COUNT().as("거래건수"),
									SUM("거래금액").as("총거래액"),
									AVG("평당거래액").as("평당거래액"),
									MAX("거래금액").as("최대거래액"),
									MIN("거래금액").as("최소거래액"))
						.update("평당거래액:int", "평당거래액=평당거래액")
						.sort("평당거래액:D")
						
						.store(RESULT)
						.build();
		
		marmot.deleteDataSet(RESULT);		
		DataSet result = marmot.createDataSet(RESULT, "the_geom", srid, plan);
		System.out.printf("elapsed: %s%n", watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 3);
	}
}
