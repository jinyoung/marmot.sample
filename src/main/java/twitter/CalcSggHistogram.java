package twitter;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.optor.geo.SpatialRelationship;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcSggHistogram {
	private static final String OUTER_LAYER = "/social/tweets/clusters";
	private static final String INNER_LAYER = "/admin/political/sgg/clusters";
	private static final String OUTPUT_LAYER = "/tmp/social/tweets/result_emd";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		Program program = Program.builder()
								.loadSpatialIndexJoin(SpatialRelationship.INTERSECTS,
													OUTER_LAYER, INNER_LAYER,
													"the_geom,id", "SIG_CD,SIG_KOR_NM")
								.groupBy("SIG_CD")
									.taggedKeyColumns("the_geom,SIG_KOR_NM")
									.count()
								.storeLayer(OUTPUT_LAYER, "the_geom", "EPSG:5186")
								.build();
		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		catalog.deleteLayer(OUTPUT_LAYER);
		marmot.execute("calc_emd_histogram", program);
	}
}
