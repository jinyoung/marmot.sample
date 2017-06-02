package geom.advanced;

import static marmot.optor.AggregateFunction.CONCAT;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

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
public class Test2017_0 {
	private static final String ADDR_BLD = "admin/address/buildings/heap";
	private static final String ADDR_BLD_UTILS_CLTS = "tmp/test2017/buildings_utils/clusters";
	private static final String GRID = "tmp/test2017/grid30";
	private static final int MAP_COUNT = 128;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		LayerInfo info = marmot.getCatalog().getLayerInfo(ADDR_BLD);
		Envelope bounds = info.getBounds();
		DimensionDouble cellSize = new DimensionDouble(30, 30);
		
		Program program = Program.builder()
								.loadSquareGridFile(bounds, cellSize, MAP_COUNT)
								.centroid("the_geom", "the_geom")
//								.aggregateJoin("the_geom", ADDR_BLD_UTILS_CLTS,
//										SpatialRelation.WITHIN_DISTANCE(2000), COUNT())
								.buffer("the_geom", "center", 100, 16)
								.aggregateJoin("center", ADDR_BLD_UTILS_CLTS,
												INTERSECTS, CONCAT("bd_mgt_sn", ","), COUNT())
								.project("the_geom,cell_id,count,csv")
								.storeLayer(GRID, "the_geom", info.getSRID())
								.build();
		marmot.deleteLayer(GRID);
		marmot.execute("get_biz_grid", program);
		
		SampleUtils.printLayerPrefix(marmot, GRID, 10);
	}
}
