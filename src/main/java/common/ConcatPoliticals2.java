package common;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.DataSet;
import marmot.Program;
import marmot.optor.JoinType;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConcatPoliticals2 {
	private static final String SID = "구역/시도";
	private static final String SGG = "구역/시군구";
	private static final String EMD = "구역/읍면동";
	private static final String LI = "구역/리";
	private static final String POLITICAL = "구역/통합법정동";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		String merge = "if ( li_cd != null ) {"
				     + "	bjd_nm = emd_kor_nm + li_kor_nm;"
				     + "    bjd_cd = li_cd;"
					 + "}"
					 + "else {"
					 + "	bjd_nm = emd_kor_nm;"
					 + "	bjd_cd = emd_cd + '00';"
					 + "	the_geom = emd_the_geom;"
					 + "}"
					 + "bjd_nm = bjd_nm.replaceAll(\"\\\\s+\",'');";
		
		String script1 = "if ( !sig_kor_nm.equals('세종특별자치시') ) {"
					   + "	bjd_nm = sig_kor_nm + bjd_nm;"
					   + "}"
					   + "bjd_nm = bjd_nm.replaceAll(\"\\\\s+\",'');";
		
		DataSet info = marmot.getDataSet(SGG);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();

		Program program;
		program = Program.builder("merge_politicals")
						.load(LI)
						.update("emd_cd2:string", "emd_cd2 = li_cd.substring(0,8)")
						.join("emd_cd2", EMD, "emd_cd",
							"*, param.the_geom as emd_the_geom,param.*-{the_geom}",
							opts->opts.joinType(JoinType.RIGHT_OUTER_JOIN).workerCount(1)
						)
						.update("bjd_nm:string,bjd_cd:string",merge)
						.project("the_geom,bjd_cd,bjd_nm,"
								+ "emd_cd,emd_kor_nm as emd_nm,"
								+ "li_cd,li_kor_nm as li_nm")
						.update("sig_cd2:string", "sig_cd2 = bjd_cd.substring(0,5)")
						.join("sig_cd2", SGG, "sig_cd", "*,param.sig_kor_nm",
							opts->opts.workerCount(1)
						)
						.update(script1)
						.project("the_geom,bjd_cd,bjd_nm,"
								+ "sig_cd2 as sgg_cd,sig_kor_nm as sgg_nm,"
								+ "emd_cd,emd_nm,li_cd,li_nm")
						.update("sid_cd2:string", "sid_cd2 = bjd_cd.substring(0,2)")
						.join("sid_cd2", SID, "ctprvn_cd", "*,param.{ctp_kor_nm,ctprvn_cd}",
							opts->opts.workerCount(1)
						)
						.update("bjd_nm = ctp_kor_nm + bjd_nm")
						.project("the_geom,bjd_cd,bjd_nm,"
								+ "ctprvn_cd as sid_cd,ctp_kor_nm as sid_nm,"
								+ "sgg_cd,sgg_nm,emd_cd,emd_nm,li_cd,li_nm")
						.store(POLITICAL)
						.build();
		
		StopWatch watch = StopWatch.start();
		marmot.deleteDataSet(POLITICAL);
		DataSet result = marmot.createDataSet(POLITICAL, geomCol, srid, program);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
	}
}
