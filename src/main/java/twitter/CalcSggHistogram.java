package twitter;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.RemotePlan;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcSggHistogram {
	private static final String LEFT_DATASET = "로그/social/twitter";
	private static final String RIGHT_DATASET = "구역/시군구";
	private static final String OUTPUT_DATASET = "/tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Plan plan = marmot.planBuilder("calc_emd_histogram")
								.loadSpatialIndexJoin(LEFT_DATASET, RIGHT_DATASET,
											SpatialRelation.INTERSECTS,
											"left.{id},right.{the_geom,SIG_CD,SIG_KOR_NM}")
								.groupBy("SIG_CD")
									.taggedKeyColumns("the_geom,SIG_KOR_NM")
									.count()
								.store(OUTPUT_DATASET)
								.build();
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		marmot.deleteDataSet(OUTPUT_DATASET);
		marmot.createDataSet(OUTPUT_DATASET, "the_geom", "EPSG:5186", plan);
	}
}
