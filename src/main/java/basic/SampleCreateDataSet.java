package basic;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;

import com.google.common.collect.Lists;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.geo.GeoFunctions;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.rset.RecordSets;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleCreateDataSet {
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotCommands.getMarmotHost(cl);
		int port = MarmotCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		// 생성될 데이터세트의 스키마를 정의함.
		RecordSchema schema = RecordSchema.builder()
											.addColumn("the_geom", DataType.POINT)
											.addColumn("name", DataType.STRING)
											.addColumn("age", DataType.INT)
											.addColumn("gpa", DataType.DOUBLE)
											.build();
		
		// 생성할 데이터세트에 저장될 레코드를 담을 레코드 객체를 생성
		Record record = DefaultRecord.of(schema);
		
		// 생성할 데이터세트에 저장될 레코드들의 리스트를 생성.
		List<Record> recordList = Lists.newArrayList();
		
		// 첫번째 레코드 (컬럼 이름을 통한 설정 )
		record.set("the_geom", GeoFunctions.ST_Point(1.0, 2.0));
		record.set("name", "홍길동");
		record.set("age", 15);
		record.set("gpa", 2.8);
		recordList.add(record.duplicate());

		// 두번째 레코드 (컬럼 번호를 통한 설정)
		record.clear();
		record.set(0, GeoFunctions.ST_Point(7.0, 9.5));
		record.set(1, "성춘향");
		record.set(2, 18);
		record.set(3, 2.5);
		recordList.add(record.duplicate());

		// 세번째 레코드 (컬럼 값 리스트를 통한 설정)
		record.clear();
		record.setAll(Arrays.asList(GeoFunctions.ST_Point(7.0, 9.5), "이몽룡", 19, 3.1));
		recordList.add(record.duplicate());
		
		// 생성된 레코드들을 이용하여 레코드 세트 생성
		RecordSet rset = RecordSets.from(schema, recordList);
		
		marmot.deleteDataSet("tmp/test");
		
		// 생성된 레코드 세트를 저장할 데이터세트를 생성한다.
		DataSet ds = marmot.createDataSet("tmp/test", record.getSchema(), "the_geom",
										"EPSG:5186");
		// 생성된 레코드 세트를 지정된 데이터세트에 저장시킨다.
		ds.append(rset);
		
		SampleUtils.printPrefix(marmot.getDataSet("tmp/test"), 10);
		marmot.disconnect();
	}
}
