package basic;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import marmot.Program;
import marmot.Record;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.geo.catalog.LayerInfo;
import marmot.optor.geo.SpatialRelationship;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class WithinDistanceJoin {
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
		Envelope bounds = GeoClientUtils.expandBy(innerInfo.getBounds(), 50);
		Geometry key = GeoClientUtils.toPolygon(bounds);
		
		Program program = Program.builder()
								.loadLayer(CADASTRAL, "intersects", key)
								.filterMatchExists("the_geom",
													SpatialRelationship.WITHIN_DISTANCE(50),
													SUBWAYS)
								.storeLayer(RESULT, "the_geom", "EPSG:5186")
								.build();

		catalog.deleteLayer(RESULT);
		marmot.execute("within_distance", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer(RESULT);
		Record record = DefaultRecord.of(rset.getRecordSchema());
		int count = 0;
		while ( ++count <= 10 && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
