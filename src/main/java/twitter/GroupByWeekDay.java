package twitter;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.RemotePlan;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GroupByWeekDay {
	private static final String TWEETS = "로그/social/twitter";
	private static final String RESULT = "/tmp/result";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		StopWatch watch = StopWatch.start();

		marmot.deleteFile(RESULT);

		Plan plan = marmot.planBuilder("group_by_weekday_and_count")
								.load(TWEETS)
								.project("id,created_at")
								.update("week_day:int", "week_day = ST_DTWeekDay(created_at)")
								.groupBy("week_day").count()
								.skip(0)
								.storeAsCsv(RESULT)
								.build();
		marmot.execute(plan);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}
}
