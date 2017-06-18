package geom.join;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleClipJoin {
	private static final String RESULT = "tmp/result";
	private static final String OUTER = "POI/주유소_가격";
	private static final String INNER = "시연/서울특별시";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		Program program = Program.builder("sample_clip_join")
								.load(OUTER)
								.clipJoin("the_geom", INNER)
								.storeMarmotFile(RESULT)
								.build();

		marmot.deleteFile(RESULT);
		marmot.execute(program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printMarmotFilePrefix(marmot, RESULT, 10);
	}
}
