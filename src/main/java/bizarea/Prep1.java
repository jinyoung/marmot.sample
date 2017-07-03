package bizarea;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Prep1 {
	private static final String BLOCKS = "구역/지오비전_집계구";
	private static final String BLOCK_CENTERS = "tmp/bizarea/centers";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = cl.getOptionValue("host", "localhost");
		int port = cl.getOptionInt("port", 12985);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);

		DataSet blocks = marmot.getDataSet(BLOCKS);
		String geomCol = blocks.getGeometryColumn();
		String srid = blocks.getSRID();

		Plan plan = marmot.planBuilder("to_centroid")
								.load(BLOCKS)
								.centroid(geomCol, geomCol)
								.store(BLOCK_CENTERS)
								.build();
		marmot.deleteDataSet(BLOCK_CENTERS);
		DataSet result = marmot.createDataSet(BLOCK_CENTERS, geomCol, srid, plan);
		
		SampleUtils.printPrefix(result, 10);
	}
}
