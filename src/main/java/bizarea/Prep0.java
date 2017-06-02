package bizarea;

import org.apache.log4j.PropertyConfigurator;

import basic.SampleUtils;
import marmot.MarmotDataSet;
import marmot.Program;
import marmot.geo.catalog.LayerInfo;
import marmot.remote.RemoteMarmotConnector;
import marmot.remote.robj.MarmotClient;
import marmot.remote.robj.RemoteMarmotDataSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Prep0 {
	private static final String SID = "admin/political/sid/heap";
	private static final String SGG = "admin/political/sgg/heap";
	private static final String SID_SGG = "admin/political/sid_sgg/heap";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		// 원격 MarmotServer에 접속.
		RemoteMarmotConnector connector = new RemoteMarmotConnector();
		MarmotClient marmot = connector.connect("localhost", 12985);
		
		LayerInfo info = marmot.getCatalog().getLayerInfo(SGG);
		String geomCol = info.getGeometryColumn();
		String srid = info.getSRID();
		
		MarmotDataSet sid = RemoteMarmotDataSet.layer(SID);

		Program program;
		program = Program.builder()
						.loadLayer(SGG)
						.update("sid_cd:string", "sid_cd = sig_cd.substring(0,2)")
						.join("sid_cd", sid, "ctprvn_cd", "*,param.ctp_kor_nm as sid_nm",
							opts->opts.workerCount(1)
						)
						.project("the_geom, sig_cd as sgg_cd, sig_kor_nm as sgg_nm, sid_cd, sid_nm")
						.storeLayer(SID_SGG, geomCol, srid)
						.build();
		marmot.deleteLayer(SID_SGG);
		marmot.execute("test", program);
		
		SampleUtils.printLayerPrefix(marmot, SID_SGG, 10);
	}
}
