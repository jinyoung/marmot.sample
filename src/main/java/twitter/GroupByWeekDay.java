package twitter;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.remote.MarmotClient;
import marmot.remote.RemoteMarmotConnector;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GroupByWeekDay {
	private static final String INPUT_LAYER = "/social/tweets/heap";
	private static final String OUTPUT_PATH = "tmp/social/tweets/result_week";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		StopWatch watch = StopWatch.start();

		marmot.deleteFile(OUTPUT_PATH);

		Program program = Program.builder()
								.loadLayer(INPUT_LAYER)
								.project("id,created_at")
								.transform("week_day:int", "week_day = ST_DTWeekDay(created_at)")
								.groupBy("week_day").count()
								.skip(0)
								.storeAsCsv(OUTPUT_PATH)
								.build();
		marmot.execute("group_by_weekday_and_count", program);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}
}
