package geom;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import marmot.Program;
import marmot.geo.GeoClientUtils;
import marmot.geo.catalog.LayerInfo;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleIndexedRangeQuery {
	private static final String RESULT = "tmp/result";
	private static final String SUBWAYS = "transit/subway_stations/clusters";
	private static final String CADASTRAL = "admin/cadastral/clusters";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		LayerInfo innerInfo = catalog.getLayerInfo(SUBWAYS);
		Envelope bounds = GeoClientUtils.expandBy(innerInfo.getBounds(), -10000);
		Geometry key = GeoClientUtils.toPolygon(bounds);
		
		Program program = Program.builder()
								.loadLayer(CADASTRAL, SpatialRelation.INTERSECTS, key)
								.project("the_geom,INNB")
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("sample_indexed_rangequery", program);
	}
}
