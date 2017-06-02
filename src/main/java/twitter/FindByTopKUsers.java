package twitter;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.optor.AggregateFunction;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteCatalog;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindByTopKUsers {
	private static final String INPUT_LAYER = "/social/tweets/heap";
	private static final String OUTPUT_LAYER = "/tmp/social/tweets/result_topk";

	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		RemoteCatalog catalog = marmot.getCatalog();
		
		StopWatch watch = StopWatch.start();

		List<String> userIds = findTopKUsers(marmot);
		System.out.println("topk_users=" + userIds);
		
		
		LayerInfo info = catalog.getLayerInfo(INPUT_LAYER);
		marmot.deleteLayer(OUTPUT_LAYER);
			
		String userIdsStr = userIds.stream().collect(Collectors.joining(","));
		String inializer = String.format("$target_users = Lists.newArrayList(%s)", userIdsStr);
		String pred = "$target_users.contains(user_id)";
		Program program = Program.builder()
								.loadLayer(INPUT_LAYER)
								.filter(inializer, pred)
								.project("the_geom,id")
								.storeLayer(OUTPUT_LAYER, "the_geom", info.getSRID())
								.build();
		marmot.execute("find_by_userids", program);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedTimeString());
	}

	public static List<String> findTopKUsers(MarmotClient marmot) throws Exception {
		// 가장 자주 tweet을 한 사용자 식별자들을 저장할 임시 파일 이름을 생성한다.
		String tempFile = "tmp/" + UUID.randomUUID().toString();

		Program program = Program.builder()
								.loadLayer(INPUT_LAYER)
								.groupBy("user_id").aggregate(AggregateFunction.COUNT())
								.pickTopK("count:D", 5)
								.store(tempFile)
								.build();
		marmot.execute("list_topk_users", program);
		
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
