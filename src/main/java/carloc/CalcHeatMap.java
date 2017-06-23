package carloc;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.HistogramCounter;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcHeatMap {
	private static final String TAXI_LOG = "로그/나비콜";
	private static final String SEOUL = "시연/서울특별시";
	private static final String RESULT = "tmp/result";
	private static final String SRID = "EPSG:5186";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet borderLayer = marmot.getDataSet(SEOUL);
		Envelope envl = borderLayer.getBounds();
		Polygon key = GeoClientUtils.toPolygon(envl);
		
		DimensionDouble cellSize = new DimensionDouble(envl.getWidth() / 30,
														envl.getHeight() / 30);
		
		Program program = Program.builder("calc_heat_map")
								.loadSquareGridFile(envl, cellSize)
								.buildSpatialHistogram("the_geom", TAXI_LOG,
													HistogramCounter.COUNT, null, "count")
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", SRID, program);
		
		SampleUtils.printPrefix(result, 5);
	}
}
