package geom.join;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleSpatialJoin {
	private static final String RESULT = "tmp/result";
	private static final String BUS_STOPS = "POI/주유소_가격";
	private static final String EMD = "구역/읍면동";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Plan plan = marmot.planBuilder("spatial_join")
								.load(BUS_STOPS)
								.spatialJoin("the_geom", EMD, SpatialRelation.INTERSECTS,
											"*-{EMD_CD},param.*-{the_geom}")
								.storeMarmotFile(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute(plan);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
