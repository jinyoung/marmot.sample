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
public class SampleLoadSpatialIndexJoin {
	private static final String RESULT = "tmp/result";
	private static final String OUTER = "transit/subway/stations/clusters";
	private static final String INNER = "admin/political/sgg/clusters";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Program program = Program.builder()
								.loadSpatialIndexJoin(OUTER, INNER, SpatialRelation.INTERSECTS,
														"left.*,right.{the_geom as the_geom2}")
								.intersection("the_geom", "the_geom2", "the_geom", 1)
								.project("*-{the_geom2}")
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("load_spatial_index_join", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
