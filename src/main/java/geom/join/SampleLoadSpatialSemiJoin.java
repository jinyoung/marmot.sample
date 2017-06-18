package geom.join;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadSpatialSemiJoin {
	private static final String RESULT = "tmp/result";
	private static final String SUBWAYS = "교통/지하철/서울역사";
	private static final String BUS_STOPS = "교통/버스/서울정류장";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Program program = Program.builder("within_distance")
								.loadSpatialLeftSemiJoin(BUS_STOPS, SUBWAYS,
														SpatialRelation.WITHIN_DISTANCE(30))
								.storeMarmotFile(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute(program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
