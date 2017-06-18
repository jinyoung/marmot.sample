package geom;

import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.DimensionDouble;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleLoadSquareGridFile {
	private static final String INPUT = "교통/지하철/서울역사";
	private static final String RESULT = "tmp/result";
	private static final double SIDE_LEN = 100;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		DataSet dataset = marmot.getDataSet(INPUT);
		String srid = dataset.getSRID();
		String geomCol = dataset.getGeometryColumn();
		DimensionDouble dim = new DimensionDouble(SIDE_LEN, SIDE_LEN);

		Program program = Program.builder()
								.loadSquareGridFile(INPUT, dim)
								.spatialSemiJoin("the_geom", INPUT, INTERSECTS)
								.store(RESULT)
								.build();

		marmot.deleteDataSet(RESULT);
		DataSet result = marmot.createDataSet(RESULT, geomCol, srid, program);
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 10);
	}
}
