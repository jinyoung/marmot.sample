package carloc;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteCatalog;
import utils.CommandLine;
import utils.CommandLineParser;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportTaxiTripLogs {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("import_taxi_trip_logs ");
		parser.addArgOption("carloc_hst", "name", "taxi loc history file", true);
		parser.addArgOption("output", "name", "output layer name", true);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}
		
		String carLocHst = cl.getOptionValue("carloc_hst");
		String output = cl.getOptionValue("output");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();

		Program program = Program.builder()
								.loadTextFile(carLocHst)
								.transformWithResourceScript("classpath:carloc/ParseTaxiTripLog.xml")
								.transformCRS("the_geom", "EPSG:4326", "EPSG:5186")
								.skip(0)
								.storeLayer(output, "the_geom", "EPSG:5186")
								.build();

		marmot.deleteLayer(output);
		marmot.execute("import_taxi_trip_logs", program);
	}
}
