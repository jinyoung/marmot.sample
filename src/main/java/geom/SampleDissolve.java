package geom;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleDissolve {
	private static final String RESULT = "tmp/sample/result";
	private static final String INPUT = "교통/지하철/서울역사";
	private static final int NPARTS = 11;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Plan plan = marmot.planBuilder("dissolve")
								.load(INPUT)
								.project("the_geom,sig_cd")
								.dissolve("sig_cd", NPARTS)
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, "the_geom", "EPSG:5186", plan);

		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
