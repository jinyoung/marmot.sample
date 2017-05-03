package geom;

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
public class SampleLoadHexagonGridFile {
	private static final String INPUT = "transit/subway/stations/heap";
	private static final String RESULT = "tmp/result";
	private static final double SIDE_LEN = 100;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(INPUT);
		String srid = info.getSRID();
		String geomCol = info.getGeometryColumn();
		Envelope bounds = info.getBounds();
		bounds.expandBy(2*SIDE_LEN, SIDE_LEN);

		Program program = Program.builder()
								.loadHexagonGridFile(srid, bounds, SIDE_LEN, 8)
								.spatialSemiJoin("the_geom", INPUT, INTERSECTS)
								.storeLayer(RESULT, geomCol, srid)
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("sample", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
