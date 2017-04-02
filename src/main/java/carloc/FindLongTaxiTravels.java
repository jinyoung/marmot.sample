package carloc;

import org.apache.log4j.PropertyConfigurator;

import utils.CommandLine;
import utils.CommandLineParser;
import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindLongTaxiTravels {
	private static final String SRID = "EPSG:5186";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("find_long_travels ");
		parser.addArgOption("taxi_trj", "path", "input taxi trajectory path", true);
		parser.addArgOption("output", "path", "output file path", true);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String taxiTrajectoryPath = cl.getOptionValue("taxi_trj");
		String outputLayerName = cl.getOptionValue("output");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();

		Program program = Program.builder()
								.loadMarmotFile(taxiTrajectoryPath)
								.filter("status == 3")
								.transformWithResourceScript("classpath:carloc/FromTrajectory.xml")
								.pickTopK("length:D", 10)
								.storeLayer(outputLayerName, "the_geom", SRID)
								.build();

		marmot.deleteLayer(outputLayerName);
		marmot.execute("find_long_travels", program);
	}
}
