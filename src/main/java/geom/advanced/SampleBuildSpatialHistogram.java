package geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.optor.geo.HistogramCounter;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleBuildSpatialHistogram {
	private static final String INPUT = "taxi/trip/heap";
	private static final String BORDER = "admin/seoul_emd/heap";
	private static final String RESULT = "tmp/result";
	private static final String TEMP_LAYER = "tmp/temp_taxi_log";
	private static final String TEMP_CLUSTER = TEMP_LAYER + "_clusters";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		LayerInfo borderLayer = catalog.getLayerInfo(BORDER);
		Envelope envl = borderLayer.getBounds();
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 50,
														envl.getHeight() / 50);
		
		Program program = Program.builder()
								.loadFishnetGridFile(envl, cellSize, 1)
								.buildSpatialHistogram("the_geom", TEMP_CLUSTER,
													HistogramCounter.COUNT, null, "count")
								.transform("cell_pos:string", "cell_pos = cell_pos.toString()")
								.storeLayer(RESULT, "the_geom", borderLayer.getSRID())
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("assign_fishnet_gridcell", program);
	}
}
