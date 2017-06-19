package geom;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleIndexedRangeQuery {
	private static final String RESULT = "tmp/result";
	private static final String SEOUL = "교통/지하철/서울역사";
	private static final String BUILDINGS = "주소/건물POI";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet info = marmot.getDataSet(SEOUL);
		Envelope bounds = GeoClientUtils.expandBy(info.getBounds(), -10000);
		Geometry key = GeoClientUtils.toPolygon(bounds);
		
		Program program = Program.builder()
								.load(BUILDINGS, SpatialRelation.INTERSECTS, key)
								.project("the_geom,시군구코드,건물명")
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 50);
	}
}
