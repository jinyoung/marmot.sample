package bizarea;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Prep1 {
	private static final String BLOCKS = "geo_vision/blocks/heap";
	private static final String BLOCK_CENTERS = "geo_vision/block_centers/heap";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		LayerInfo info = marmot.getCatalog().getLayerInfo(BLOCKS);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();

		Program program = Program.builder()
								.loadLayer(BLOCKS)
								.centroid(geomCol, geomCol)
								.storeLayer(BLOCK_CENTERS, geomCol, srid)
								.build();
		marmot.deleteLayer(BLOCK_CENTERS);
		marmot.execute("test", program);
		
		SampleUtils.printLayerPrefix(marmot, BLOCK_CENTERS, 10);
	}
}
