package common;

import java.util.UUID;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.Plan;
import marmot.optor.JoinOptions;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BuildJinBunPOI {
	private static final String JIBUN = "주소/지번";
	private static final String ADDR = "주소/주소";
	private static final String BUILD_POI = "주소/건물POI";
	private static final String RESULT = "주소/지번POI";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);

		Plan plan;
		DataSet result;

		StopWatch watch = StopWatch.start();
		String tempDs = "tmp/" + UUID.randomUUID().toString();
		plan = marmot.planBuilder("distinct_jibun")
						.load(JIBUN)
						.distinct("건물관리번호", 11) 
						.store(tempDs)
						.build();
		result = marmot.createDataSet(tempDs, plan);

		try {
			DataSet info = marmot.getDataSet(BUILD_POI);
			String geomCol = info.getGeometryColumn();
			String srid = info.getSRID();
			
			plan = marmot.planBuilder("build_jibun_poi")
							.load(BUILD_POI)
							.project(geomCol + ",도로명코드,건물본번,건물부번,지하여부,법정동코드")
							.join("도로명코드,건물본번,건물부번,지하여부",
									ADDR, "도로명코드,건물본번,건물부번,지하여부",
									geomCol + ",param.{건물관리번호}",
									new JoinOptions().workerCount(23))
							.join("건물관리번호", tempDs, "건물관리번호",
									"*,param.{법정동코드,지번본번,지번부번,산여부}", null)
							.distinct("건물관리번호,법정동코드,지번본번,지번부번,산여부")
							.store(RESULT)
							.build();
			
			marmot.deleteDataSet(RESULT);
			result = marmot.createDataSet(RESULT, geomCol, srid, plan);
			System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
			
			SampleUtils.printPrefix(result, 5);
		}
		finally {
			marmot.deleteDataSet(tempDs);
		}
	}
}
