package basic;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUpdate {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Program program = Program.builder("update")
								.load(INPUT)
								.update("the_geom:point,area:double,sig_cd:int",
											"area = ST_Area(the_geom);"
											+ "the_geom = ST_Centroid(the_geom);"
											+ "sig_cd=Integer.parseInt(sig_cd);"
											+ "kor_sub_nm='Station(' + kor_sub_nm + ')'")
								.project("the_geom,area,sig_cd,kor_sub_nm")
								.storeMarmotFile(RESULT)
								.build();
		marmot.deleteFile(RESULT);
		marmot.execute(program);
		
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
