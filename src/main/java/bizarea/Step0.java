package bizarea;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step0 {
	private static final String LAND_USAGE = "토지/용도지역지구";
	private static final String POLITICAL = "구역/통합법정동";
	private static final String TEMP_BIG_CITIES = "tmp/bizarea/big_cities";
	private static final String TEMP_BIZ_AREA = "tmp/bizarea/area";
	private static final String BLOCK_CENTERS = "tmp/bizarea/centers";
	private static final String BIZ_GRID = "tmp/bizarea/grid100";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet result;
		
		//  행정경계에서 대도시 영역을 추출한다.
		result = filterBigCities(marmot, TEMP_BIG_CITIES);
		
		// 용도지구에서 상업지역 추출
		result = filterBizArea(marmot, TEMP_BIZ_AREA);

		DataSet info = marmot.getDataSet(LAND_USAGE);
		String srid = info.getSRID();
		Envelope bounds = info.getBounds();
		DimensionDouble cellSize = new DimensionDouble(100, 100);
		
		Plan plan = RemotePlan.builder("get_biz_grid")
								// 용도지구에 대한 100m 크기의 그리드를 생성 
								.loadSquareGridFile(bounds, cellSize)
								// 상업지구에 겹치는 그리드 셀만 추출한다.
								.spatialSemiJoin("the_geom", TEMP_BIZ_AREA, INTERSECTS)
								// 상업지구 그리드 셀에 대해 대도시 영역만을 선택하고,
								// 행정도 코드(sgg_cd)를 부여한다.
								.spatialJoin("the_geom", TEMP_BIG_CITIES, INTERSECTS,
											"*-{cell_pos},param.sgg_cd")
								// 소지역 코드 (block_cd)를 부여한다.
								.spatialJoin("the_geom", BLOCK_CENTERS, INTERSECTS,
											"*-{cell_pos},param.block_cd")
								.store(BIZ_GRID)
								.build();
		marmot.deleteDataSet(BIZ_GRID);
		result = marmot.createDataSet(BIZ_GRID, "the_geom", srid, plan);
		
		marmot.deleteDataSet(TEMP_BIG_CITIES);
		marmot.deleteDataSet(TEMP_BIZ_AREA);
		
		SampleUtils.printPrefix(result, 10);
	}

	private static final DataSet filterBigCities(MarmotClient marmot, String result) {
		String listExpr1 = Arrays.asList("11","26","27", "28", "29", "30", "31")
								.stream()
								.map(str -> "'" + str + "'")
								.collect(Collectors.joining(",", "[", "]"));
		String listExpr2 = Arrays.asList("41115","41111","41117", "41113", "48125", "48123",
								"48127", "48121", "48129", "41281", "41285", "41287")
								.stream()
								.map(str -> "'" + str + "'")
								.collect(Collectors.joining(",", "[", "]"));
		String initExpr = String.format("$sid_cd=%s; $sgg_cd=%s", listExpr1, listExpr2);

		DataSet political = marmot.getDataSet(POLITICAL);
		String geomCol = political.getGeometryColumn();
		String srid = political.getSRID();
		
		Plan plan = RemotePlan.builder("filter_big_cities")
								.load(POLITICAL)
								.update("sid_cd:string,sgg_cd:string",
										"sid_cd = bjd_cd.substring(0,2);"
										+ "sgg_cd = bjd_cd.substring(0,5);")
								.filter(initExpr,
										"$sid_cd.contains(sid_cd) || $sgg_cd.contains(sgg_cd)")
								.store(result)
								.build();
		marmot.deleteDataSet(result);
		return marmot.createDataSet(result, geomCol, srid, plan);
	}

	private static final DataSet filterBizArea(MarmotClient marmot, String result)
		throws Exception {
		String listExpr = Arrays.asList("일반상업지역","유통상업지역","근린상업지역",
										"중심상업지역")
								.stream()
								.map(str -> "'" + str + "'")
								.collect(Collectors.joining(",", "[", "]"));
		String initExpr = String.format("$types = %s", listExpr);
		
		DataSet info = marmot.getDataSet(LAND_USAGE);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();

		Plan plan = RemotePlan.builder("filter_biz_area")
								.load(LAND_USAGE)
								.filter(initExpr, "$types.contains(dgm_nm)")
								.project("the_geom")
								.store(TEMP_BIZ_AREA)
								.build();
		marmot.deleteDataSet(result);
		return marmot.createDataSet(result, geomCol, srid, plan);
	}
}
