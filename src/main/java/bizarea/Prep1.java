package bizarea;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Prep1 {
	private static final String BLOCKS = "구역/지오비전_집계구";
	private static final String BLOCK_CENTERS = "tmp/bizarea/centers";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		DataSet blocks = marmot.getDataSet(BLOCKS);
		String geomCol = blocks.getGeometryColumn();
		String srid = blocks.getSRID();

		Program program = Program.builder("to_centroid")
								.load(BLOCKS)
								.centroid(geomCol, geomCol)
								.store(BLOCK_CENTERS)
								.build();
		marmot.deleteDataSet(BLOCK_CENTERS);
		DataSet result = marmot.createDataSet(BLOCK_CENTERS, geomCol, srid, program);
		
		SampleUtils.printPrefix(result, 10);
	}
}
