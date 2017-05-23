package carloc;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.optor.geo.AggregateFunction;
import marmot.optor.geo.SpatialRelation;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteCatalog;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.io.ResourceUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindHotHospitals {
	private static final String SRID = "EPSG:5186";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("find_hot_hospitals ");
		parser.addArgOption("taxi_log", "name", "taxi trip layer name", true);
		parser.addArgOption("hospital", "name", "input hospitals layer name", true);
		parser.addArgOption("output", "path", "output file path", true);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}
		
		String taxiTripLogLayer = cl.getOptionValue("taxi_log");
		String hospitalLayerName = cl.getOptionValue("hospital");
		String outputLayerName = cl.getOptionValue("output");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();

		List<String> lines = null;
		try ( InputStream is = ResourceUtils.openInputStream("classpath:carloc/ParseTaxiTripLog.xml");
				BufferedReader reader = new BufferedReader(new InputStreamReader(is)) ) {
			lines = reader.lines().collect(Collectors.toList());
		}
		
		Program program = Program.builder()
								.loadLayer(taxiTripLogLayer)
								.filter("status==1 || status==2")
								.spatialJoin("the_geom", hospitalLayerName,
											SpatialRelation.WITHIN_DISTANCE(50),
											"param.{the_geom,gid,bplc_nm,bz_stt_nm}")
								.filter("bz_stt_nm=='운영중'")
								.groupBy("gid")
									.taggedKeyColumns("the_geom,bplc_nm")
									.aggregate(AggregateFunction.COUNT())
								.rank("count:D", "rank")
								.storeLayer(outputLayerName, "the_geom", SRID)
								.build();

		marmot.deleteLayer(outputLayerName);
		marmot.execute("find_hot_hospitals", program);
	}
}
