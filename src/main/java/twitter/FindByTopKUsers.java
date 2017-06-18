package twitter;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindByTopKUsers {
	private static final String TWEETS = "로그/social/twitter";
	private static final String RESULT = "/tmp/result";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		StopWatch watch = StopWatch.start();

		List<String> userIds = findTopKUsers(marmot);
		System.out.println("topk_users=" + userIds);
				
		DataSet info = marmot.getDataSet(TWEETS);
			
		String userIdsStr = userIds.stream().collect(Collectors.joining(","));
		String inializer = String.format("$target_users = Lists.newArrayList(%s)", userIdsStr);
		String pred = "$target_users.contains(user_id)";
		Program program = Program.builder("find_by_userids")
								.load(TWEETS)
								.filter(inializer, pred)
								.project("the_geom,id")
								.store(RESULT)
								.build();
		marmot.deleteDataSet(RESULT);
		marmot.createDataSet(RESULT, "the_geom", info.getSRID(), program);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}

	public static List<String> findTopKUsers(MarmotClient marmot) throws Exception {
		// 가장 자주 tweet을 한 사용자 식별자들을 저장할 임시 파일 이름을 생성한다.
		String tempFile = "tmp/" + UUID.randomUUID().toString();

		Program program = Program.builder("list_topk_users")
								.load(TWEETS)
								.groupBy("user_id").aggregate(AggregateFunction.COUNT())
								.pickTopK("count:D", 5)
								.storeMarmotFile(tempFile)
								.build();
		marmot.execute(program);
		SampleUtils.printMarmotFilePrefix(marmot, tempFile, 10);
		
		try {
			return marmot.readMarmotFile(tempFile)
						.stream()
						.map(rec -> rec.getString("user_id"))
						.collect(Collectors.toList());
		}
		finally {
			marmot.deleteFile(tempFile);
		}
	}
}
