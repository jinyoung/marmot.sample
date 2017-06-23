package geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.optor.geo.HistogramCounter;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleBuildSpatialHistogram {
	private static final String STATIONS = "POI/주유소_가격";
	private static final String BORDER = "시연/서울특별시";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet borderLayer = marmot.getDataSet(BORDER);
		Envelope envl = borderLayer.getBounds();
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 30,
														envl.getHeight() / 30);
		
		Program program = Program.builder("build_spatial_histogram")
								// 서울특별시 구역을 기준으로 사각 그리드를 생성함.
								.loadSquareGridFile(BORDER, cellSize)
								// 사각 그리드 셀 중에서 서울특별시 영역만 필터링.
								.spatialSemiJoin("the_geom", BORDER, SpatialRelation.INTERSECTS)
								// 사각 그리드 셀 중에서 주유소와 겹치는 셀만 필터링.
//								.spatialSemiJoin("the_geom", STATIONS, SpatialRelation.INTERSECTS)
								.buildSpatialHistogram("the_geom", STATIONS,
													HistogramCounter.COUNT, null, "count")
								.update("cell_pos:string,count:int",
										"cell_pos = cell_pos.toString(); count=count;")
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", borderLayer.getSRID(), program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
