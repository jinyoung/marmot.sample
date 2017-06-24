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
public class SampleAssignSquareGridCell {
	private static final String INPUT = "POI/주유소_가격";
	private static final String BORDER = "시연/서울특별시";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet border = marmot.getDataSet(BORDER);
		Envelope envl = border.getBounds();
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 100,
														envl.getHeight() / 100);
		
		Plan plan = RemotePlan.builder("assign_fishnet_gridcell")
								.load(INPUT)
								.assignSquareGridCell("the_geom", envl, cellSize)
								.update("count:int", "count = 1")
								.groupBy("cell_id")
									.taggedKeyColumns("cell_geom,cell_pos")
									.workerCount(11)
									.aggregate(AggregateFunction.SUM("count").as("count"))
								.update("x:int,y:int", "x = cell_pos.x; y = cell_pos.y")
								.project("cell_geom as the_geom,x,y,count")
								.storeMarmotFile(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
