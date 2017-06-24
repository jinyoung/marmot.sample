package geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test2017_2 {
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
		
		Plan plan = RemotePlan.builder("get_biz_grid")
								.load(ADDR_BLD_UTILS)
								.buffer("the_geom", "buffer", 100, 16)
								.assignSquareGridCell("buffer", bounds, cellSize)
								.centroid("cell_geom", "cell_geom")
								.intersects("cell_geom", "the_geom")
								.groupBy("cell_id")
									.taggedKeyColumns("cell_geom")
									.aggregate(AggregateFunction.COUNT())
								.project("cell_geom as the_geom,*-{cell_geom}")
								.store(GRID)
								.build();
		marmot.deleteDataSet(GRID);
		DataSet result = marmot.createDataSet(GRID, "the_geom", info.getSRID(), plan);
		
		SampleUtils.printPrefix(result, 5);
	}
}
