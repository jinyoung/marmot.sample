package geom.join;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
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
	private static final String OUTER = "교통/지하철/서울역사";
	private static final String INNER = "구역/시군구";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Program program = Program.builder("load_spatial_index_join")
								.loadSpatialIndexJoin(OUTER, INNER, SpatialRelation.INTERSECTS,
														"left.*,right.{the_geom as the_geom2}")
								.intersection("the_geom", "the_geom2", "the_geom", 1)
								.project("*-{the_geom2}")
								.storeMarmotFile(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
