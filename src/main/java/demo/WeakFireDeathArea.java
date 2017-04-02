package demo;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.Record;
import marmot.RecordSet;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.RemoteCatalog;
import marmot.support.DefaultRecord;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class WeakFireDeathArea {
	private static final String LAYER_HOSPITAL = "utility/hospitals";
	private static final String LAYER_SEOUL = "demo/demo_seoul";
	private static final String LAYER_FIRE = "report/fire_death";
	private static final String SRID = "EPSG:5186";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		// 서울시 종합병원 위치에서 3km 버퍼 연산을 취해 clustered layer를 생성한다.
		Program program0 = Program.builder()
								.loadLayer(LAYER_HOSPITAL)
								.buffer("the_geom", 3000)
								.storeLayer("tmp/fire_death/hospitals3000", "the_geom", SRID)
								.build();
		marmot.deleteLayer("tmp/fire_death/hospitals3000");
		marmot.execute("demo_process0", program0);

		// 서울시 지도에서 종합병원 3km 이내 영역과 겹치지 않은 영역을 계산한다.
		Program program1 = Program.builder()
								.loadLayer(LAYER_SEOUL)
								.differenceJoin("the_geom", "tmp/fire_death/hospitals3000")
								.storeLayer("tmp/fire_death/far_seoul", "the_geom", SRID)
								.build();
		marmot.deleteLayer("tmp/fire_death/far_seoul");
		marmot.execute("demo_process1", program1);

		// 화재피해 영역 중에서 서울 읍면동과 겹치는 부분을 clip 하고, 각 피해 영역의 중심점을 구한다.
		Program program2 = Program.builder()
								.loadLayer(LAYER_FIRE)
								.clipJoin("the_geom", LAYER_SEOUL)
								.centroid("the_geom")
								.clipJoin("the_geom", "tmp/fire_death/far_seoul")
								.storeLayer("tmp/fire_death/weak_area", "the_geom", SRID)
								.build();
		marmot.deleteLayer("tmp/fire_death/weak_area");
		marmot.execute("demo_process2", program2);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		RecordSet rset = marmot.readLayer("tmp/fire_death/weak_area");
		Record record = DefaultRecord.of(rset.getRecordSchema());
		int count = 0;
		while ( ++count <= 10 && rset.next(record) ) {
			System.out.println(record);
		}
	}
}
