package bizarea;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1Building {
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	private static final String BUILDINGS = "건물/통합정보";
	private static final String RESULT = "tmp/bizarea/grid100_land";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		avgExpr = String.format("flow_pop=(%s)/24", avgExpr);
		
		DataSet info = marmot.getDataSet(BIZ_GRID);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Plan plan = RemotePlan.builder("building_registry")
								.load(BUILDINGS)
								// BIZ_GRID와 소지역 코드를 이용하여 조인하여,
								// 대도시 상업지역과 겹치는 유동인구 구역을 뽑는다. 
								.spatialJoin("the_geom", BIZ_GRID, INTERSECTS,
											"건축물용도코드,대지면적,param.*")
								// 그리드 셀, 건축물 용도별로 건물 수와 총 면점을 집계한다. 
								.groupBy("cell_id,block_cd,건축물용도코드")
									.taggedKeyColumns(geomCol + ",sgg_cd")
									.aggregate(SUM("대지면적").as("대지면적"),
												COUNT().as("bld_cnt"))
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.store(RESULT)
								.build();
		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, plan);
		
		SampleUtils.printPrefix(result, 10);
	}
}
