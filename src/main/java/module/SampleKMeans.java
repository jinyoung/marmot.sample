package module;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Point;

import common.SampleUtils;
import marmot.DataSet;
import marmot.Plan;
import marmot.RemotePlan;
import marmot.process.geo.ClusterWithKMeansParameters;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleKMeans {
	private static final String SGG = "구역/시군구";
	private static final String INPUT = "토지/용도지역지구";
	private static final String OUTPUT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		ClusterWithKMeansParameters params = new ClusterWithKMeansParameters();
		params.layerName(INPUT);
		params.outputLayerName(OUTPUT);
		params.dataColumn("the_geom");
		params.clusterColumn("group");
		params.initialCentroids(getInitCentroids(marmot, 9, 0.025));
		params.terminationDistance(100);
		params.terminationIterations(30);
		
		marmot.deleteDataSet(OUTPUT);
		marmot.executeProcess("kmeans", params);
		
		DataSet output = marmot.getDataSet(OUTPUT);
		SampleUtils.printPrefix(output, 10);
	}
	
	private static List<Point> getInitCentroids(MarmotClient marmot, int ncentroids,
												double ratio) {
		Plan plan = marmot.planBuilder("get_init_centroids")
								.load(SGG)
								.sample(ratio)
								.limit(ncentroids)
								.project("the_geom")
								.centroid("the_geom", "the_geom")
								.build();
		return marmot.executeLocally(plan).stream()
					.map(r -> (Point)r.getGeometry(0))
					.collect(Collectors.toList());
	}
}
