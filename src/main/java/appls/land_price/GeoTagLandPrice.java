package appls.land_price;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.MarmotDataSet;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteMarmotDataSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoTagLandPrice {
	private static final String LAND_PRICE = "data/land_price";
	private static final String PNU_CODES = "admin/pnu_codes/heap";
	private static final String RESULT = "admin/land_price/heap";
	private static final int REDUCER_COUNT = 48;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(PNU_CODES);
		MarmotDataSet landPrice = RemoteMarmotDataSet.csvFiles(LAND_PRICE);

		Program program = Program.builder()
								.loadLayer(PNU_CODES)
								.join("pnu", landPrice, "pnu", "the_geom,param.*",
									opts->opts.workerCount(REDUCER_COUNT))
								.storeLayer(RESULT, "the_geom", info.getSRID())
								.build();

		marmot.deleteLayer(RESULT);
		marmot.execute("geotag_land_price", program);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
