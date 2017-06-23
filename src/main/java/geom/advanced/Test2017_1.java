package geom.advanced;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test2017_1 {
	private static final String ADDR_BLD = "건물/위치";
	private static final String ADDR_BLD_UTILS = "tmp/test2017/buildings_utils";
	private static final String GRID = "tmp/test2017/grid30";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		DataSet info = marmot.getDataSet(ADDR_BLD);
		Envelope bounds = info.getBounds();
		DimensionDouble cellSize = new DimensionDouble(30, 30);
		
		Program program = Program.builder("get_biz_grid")
								.loadSquareGridFile(ADDR_BLD, cellSize)
								.centroid("the_geom", "the_geom")
//								.aggregateJoin("the_geom", ADDR_BLD_UTILS_CLTS,
//										SpatialRelation.WITHIN_DISTANCE(2000), COUNT())
								.buffer("the_geom", "center", 100, 16)
								.aggregateJoin("center", ADDR_BLD_UTILS,
												INTERSECTS, COUNT())
								.project("the_geom,cell_id,count")
								.store(GRID)
								.build();
		marmot.deleteDataSet(GRID);
		DataSet result = marmot.createDataSet(GRID, "the_geom", info.getSRID(), program);
		
		SampleUtils.printPrefix(result, 10);
	}
}
