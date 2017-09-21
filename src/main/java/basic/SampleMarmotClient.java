package basic;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;

import marmot.PlanExecution;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.command.MarmotCommands;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.async.AsyncExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleMarmotClient {
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
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect(host, port);
		
		RecordSet rset = marmot.readMarmotFile("database/heap/교통/지하철/서울역사");
		marmot.deleteFile("tmp/result");
		marmot.writeMarmotFile("tmp/result", rset);
		
//		Plan plan = marmot.planBuilder("test")
//							.load("교통/지하철/서울역사")
//							.filter("kor_sub_nm.length() > 5")
//							.project("sub_sta_sn,kor_sub_nm")
//							.build();
//		rset = marmot.executeLocally(plan);
//		SampleUtils.printPrefix(rset, 5);
//		
//		DataSet ds = marmot.getDataSet("교통/지하철/서울역사");
//		Plan plan2 = marmot.planBuilder("test2")
//							.filter("kor_sub_nm.length() > 5")
//							.project("sub_sta_sn,kor_sub_nm")
//							.build();
//		rset = marmot.executeLocally(plan2, ds.read());
//		SampleUtils.printPrefix(rset, 5);
		
		Plan plan3 = marmot.planBuilder("test")
							.load("POI/주유소_가격")
							.filter("휘발유 > 1400;")
							.project("상호,휘발유,주소")
							.store("tmp/result")
							.build();
		marmot.deleteDataSet("tmp/result");
//		ds = marmot.createDataSet("tmp/result", plan3);
		
		PlanExecution exec;
		RecordSchema schema = marmot.getOutputRecordSchema(plan3);
		marmot.createDataSet("tmp/result", schema);
		exec = marmot.createPlanExecution(plan3);
		exec.disableLocalExecution();
		exec.start();
		System.out.println(exec.getResult());

		Plan plan4 = marmot.planBuilder("test")
							.load("POI/주유소_가격")
							.filter("Thread.sleep(50); 휘발유 > 1400;")
							.project("상호,휘발유,주소")
							.store("tmp/result")
							.build();
		marmot.deleteDataSet("tmp/result");
		marmot.createDataSet("tmp/result", schema);
		exec = marmot.createPlanExecution(plan4);
		exec.start();
		boolean to = exec.waitForDone(2, TimeUnit.SECONDS);
		if ( to ) {
			System.err.println("should be false");
		}
		exec.cancel();
		System.out.println(exec.getResult());
		
//		SampleUtils.printPrefix(ds, 5);
		
		marmot.disconnect();
	}
}
