package lx;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import basic.SampleUtils;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sample2_1 {
	private static final String SGG = "lx/sgg_seoul/heap";
	private static final String RESULT = "tmp/grid_500m_seoul";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(SGG);
		Envelope bounds = info.getBounds();
		DimensionDouble cellSize = new DimensionDouble(500, 500);

		Program program = Program.builder()
								.loadFishnetGridFile(bounds, cellSize, 1)
								.storeLayer(RESULT, "the_geom", "EPSG:5179")
								.build();
		marmot.deleteLayer(RESULT);
		marmot.execute("test", program);
		
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
