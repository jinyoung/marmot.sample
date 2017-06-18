package geom.advanced;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.Program;
import marmot.optor.geo.HistogramCounter;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleBuildSpatialHistogram {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String BORDER = "시연/서울특별시";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet borderLayer = marmot.getDataSet(BORDER);
		Envelope envl = borderLayer.getBounds();
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 50,
														envl.getHeight() / 50);
		
		Program program = Program.builder("build_spatial_histogram")
								.loadSquareGridFile(envl, cellSize)
								.buildSpatialHistogram("the_geom", TAXI_LOG,
													HistogramCounter.COUNT, null, "count")
								.update("cell_pos:string", "cell_pos = cell_pos.toString()")
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		marmot.createDataSet(RESULT, "the_geom", borderLayer.getSRID(), program);
	}
}
