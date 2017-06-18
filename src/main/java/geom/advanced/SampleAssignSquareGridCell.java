package geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.Program;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleAssignSquareGridCell {
	private static final String INPUT = "taxi/trip/heap";
	private static final String BORDER = "시연/서울특별시";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet border = marmot.getDataSet(BORDER);
		Envelope envl = border.getBounds();
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 50,
														envl.getHeight() / 50);
		
		Program program = Program.builder("assign_fishnet_gridcell")
								.load(INPUT)
								.filter("status==1 || status==2")
								.assignSquareGridCell("the_geom", envl, cellSize)
								.transform("count:int", "count = 1")
								.groupBy("cell_ordinal")
									.taggedKeyColumns("cell_geom,cell_pos")
									.workerCount(5)
									.aggregate(AggregateFunction.SUM("count").as("count"))
								.transform("x:int,y:int", "x = cell_pos.x; y = cell_pos.y")
								.project("cell_geom as the_geom,x,y,count")
								.storeMarmotFile(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute(program);
	}
}
