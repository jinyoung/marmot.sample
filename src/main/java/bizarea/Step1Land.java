package bizarea;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.SUM;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step1Land {
	private static final String BIZ_GRID = "tmp/biz/grid100";
	private static final String LAND_AREA = "admin/land/registry/area/heap";
	private static final String RESULT = "tmp/biz/grid100_land";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		String avgExpr = IntStream.range(0, 24)
								.mapToObj(idx -> String.format("avg_%02dtmst", idx))
								.collect(Collectors.joining("+"));
		avgExpr = String.format("flow_pop=(%s)/24", avgExpr);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(BIZ_GRID);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Program program = Program.builder()
								.loadLayer(LAND_AREA)
								// BIZ_GRID와 소지역 코드를 이용하여 조인하여,
								// 대도시 상업지역과 겹치는 유동인구 구역을 뽑는다. 
								.spatialJoin("the_geom", BIZ_GRID, INTERSECTS,
											"main_purps as bld_using,area,param.*")
								// 그리드 셀, 건축물 용도별로 건물 수와 총 면점을 집계한다. 
								.update("area:float", "area = area")
								.groupBy("cell_id,block_cd,bld_using")
									.taggedKeyColumns(geomCol + ",sgg_cd")
									.aggregate(SUM("area").as("bld_area"),
												COUNT().as("bld_cnt"))
								.project(String.format("%s,*-{%s}", geomCol, geomCol))
								.storeLayer(RESULT, geomCol, srid)
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("land_registry", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
