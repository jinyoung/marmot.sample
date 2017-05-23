package twitter;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcSggHistogram {
	private static final String LEFT_LAYER = "/social/tweets/clusters";
	private static final String RIGHT_LAYER = "/admin/political/sgg/clusters";
	private static final String OUTPUT_LAYER = "/tmp/social/tweets/result_emd";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		Program program = Program.builder()
								.loadSpatialIndexJoin(LEFT_LAYER, RIGHT_LAYER,
													SpatialRelation.INTERSECTS,
													"left.{the_geom,id},right.{SIG_CD,SIG_KOR_NM}")
								.groupBy("SIG_CD")
									.taggedKeyColumns("the_geom,SIG_KOR_NM")
									.count()
								.storeLayer(OUTPUT_LAYER, "the_geom", "EPSG:5186")
								.build();
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		marmot.deleteLayer(OUTPUT_LAYER);
		marmot.execute("calc_emd_histogram", program);
	}
}
