package lx;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import basic.SampleUtils;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sample2_2 {
	private static final String SGG = "lx/sgg_seoul/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(SGG);
		String srid = info.getSRID();
		Envelope bounds = info.getBounds();

		Program program = Program.builder()
								.loadHexagonGridFile(srid, bounds, 500, 1)
								.spatialSemiJoin("the_geom", SGG, INTERSECTS)
								.storeLayer(RESULT, "the_geom", srid)
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("test", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
