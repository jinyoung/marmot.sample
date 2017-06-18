package geom;

import static marmot.optor.AggregateFunction.ConvexHull;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleConvexHull {
	private static final String INPUT = "교통/버스/서울정류장";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder()
								.load(INPUT)
								.aggregate(ConvexHull("the_geom"))
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", program);

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
