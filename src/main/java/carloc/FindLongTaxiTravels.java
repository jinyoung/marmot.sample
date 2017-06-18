package carloc;

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
public class FindLongTaxiTravels {
	private static final String TAXI_TRJ = "로그/나비콜/택시경로";
	private static final String RESULT = "tmp/result";
	private static final String SRID = "EPSG:5186";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder("find_long_travels")
								.load(TAXI_TRJ)
								.filter("status == 3")
								.update("length:double",
										"length = ST_TRLength(trajectory)")
								.pickTopK("length:D", 10)
								.update("the_geom:line_string",
										"the_geom = ST_TRLineString(trajectory)")
								.project("*-{trajectory}")
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", SRID, program);
		
		SampleUtils.printPrefix(result, 5);
	}
}
