package geom;

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
public class SampleFilterEmpty {
//	private static final String INPUT = "transit/subway_stations/heap";
	private static final String INPUT = "admin/cadastral_28/heap";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(INPUT);
		String filterExpr = String.format("ST_IsEmpty(%s)", info.getGeometryColumn());

		Program program = Program.builder()
								.loadLayer(INPUT)
								.filter(filterExpr)
								.storeLayer(RESULT, info.getGeometryColumn(), info.getSRID())
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("filter_is_empty", program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printLayerPrefix(marmot, RESULT, 10);
	}
}
