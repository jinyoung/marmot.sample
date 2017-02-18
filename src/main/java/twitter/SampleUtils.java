package twitter;

import java.util.Map;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;

import marmot.remote.MarmotClient;
import twitter.SampleUtils.EmdInfo;
import twitter.SampleUtils.SggInfo;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUtils {
	private static final String SGG_LAYER = "/admin/political/sgg/heap";
	private static final String EMD_LAYER = "/admin/political/emd/heap";
	
	public static class SggInfo {
		Geometry m_geom;
		String m_name;
		
		SggInfo(Geometry geom, String name) {
			m_geom = geom;
			m_name = name;
		}
	}
	public static Map<String,SggInfo> loadSgg(MarmotClient marmot) {
		Map<String,SggInfo> infos = Maps.newHashMap();
		
		marmot.readLayer(SGG_LAYER)
				.stream()
				.forEach(rec -> {
					Geometry geom = rec.getGeometry("the_geom");
					String sigCd = rec.getString("SIG_CD");
					String sigName = rec.getString("SIG_KOR_NM");
					
					infos.put(sigCd, new SggInfo(geom, sigName));
				});
		return infos;
	}
	
	public static class EmdInfo {
		Geometry m_geom;
		String m_name;
		
		EmdInfo(Geometry geom, String name) {
			m_geom = geom;
			m_name = name;
		}
	}
	public static Map<String,EmdInfo> loadEmd(MarmotClient marmot) {
		Map<String,EmdInfo> infos = Maps.newHashMap();
		
		marmot.readLayer(EMD_LAYER)
				.stream()
				.forEach(rec -> {
					Geometry geom = rec.getGeometry("the_geom");
					String emdCd = rec.getString("EMD_CD");
					String emdName = rec.getString("EMD_KOR_NM");
					
					infos.put(emdCd, new EmdInfo(geom, emdName));
				});
		return infos;
	}

}
