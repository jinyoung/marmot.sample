package bizarea;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import basic.SampleUtils;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Step0 {
	private static final String LAND_USAGE = "admin/land_usage/heap";
	private static final String TEMP_BIG_CITIES = "tmp/biz/big_cities";
	private static final String TEMP_BIZ_AREA = "tmp/biz/area";
	private static final String BLOCK_CENTERS = "geo_vision/block_centers/heap";
	private static final String BIZ_GRID = "tmp/biz/grid100";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		//  행정경계에서 대도시 영역을 추출한다.
		filterBigCities(marmot, TEMP_BIG_CITIES);
		
		// 용도지구에서 상업지역 추출
		filterBizArea(marmot, TEMP_BIZ_AREA);

		LayerInfo info = marmot.getCatalog().getLayerInfo(LAND_USAGE);
		Envelope bounds = info.getBounds();
		DimensionDouble cellSize = new DimensionDouble(100, 100);
		
		Program program = Program.builder()
								// 용도지구에 대한 100m 크기의 그리드를 생성 
								.loadSquareGridFile(bounds, cellSize, 16)
								// 상업지구에 겹치는 그리드 셀만 추출한다.
								.spatialSemiJoin("the_geom", TEMP_BIZ_AREA, INTERSECTS)
								// 상업지구 그리드 셀에 대해 대도시 영역만을 선택하고,
								// 행정도 코드(sgg_cd)를 부여한다.
								.spatialJoin("the_geom", TEMP_BIG_CITIES, INTERSECTS,
											"*-{cell_pos},param.sgg_cd")
								// 소지역 코드 (block_cd)를 부여한다.
								.spatialJoin("the_geom", BLOCK_CENTERS, INTERSECTS,
											"*-{cell_pos},param.block_cd")
								.storeLayer(BIZ_GRID, "the_geom", info.getSRID())
								.build();
		marmot.deleteLayer(BIZ_GRID);
		marmot.execute("get_biz_grid", program);
		
		marmot.deleteLayer(TEMP_BIG_CITIES);
		marmot.deleteLayer(TEMP_BIZ_AREA);
		
		SampleUtils.printLayerPrefix(marmot, BIZ_GRID, 10);
	}

	private static final String SID_SGG = "admin/political/sid_sgg/heap";
	private static final void filterBigCities(MarmotClient marmot, String resultLayer) {
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

		LayerInfo info = marmot.getCatalog().getLayerInfo(SID_SGG);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		Program program = Program.builder()
								.loadLayer(SID_SGG)
								.filter(initExpr,
										"$sid_cd.contains(sid_cd) || $sgg_cd.contains(sgg_cd)")
								.storeLayer(TEMP_BIG_CITIES, geomCol, srid)
								.build();
		marmot.deleteLayer(TEMP_BIG_CITIES);
		marmot.execute("filter_big_cities", program);
	}

	private static final void filterBizArea(MarmotClient marmot, String resultLayer)
		throws Exception {
		String listExpr = Arrays.asList("일반상업지역","유통상업지역","근린상업지역",
										"중심상업지역")
								.stream()
								.map(str -> "'" + str + "'")
								.collect(Collectors.joining(",", "[", "]"));
		String initExpr = String.format("$types = %s", listExpr);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(LAND_USAGE);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();

		Program program = Program.builder()
								.loadLayer(LAND_USAGE)
								.filter(initExpr, "$types.contains(dgm_nm)")
								.project("the_geom")
								.storeLayer(TEMP_BIZ_AREA, geomCol, srid)
								.build();
		marmot.deleteLayer(TEMP_BIZ_AREA);
		marmot.execute("filter_biz_area", program);
	}
}
