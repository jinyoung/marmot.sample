package vworld;

import org.apache.log4j.Logger;

import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.RecordTransform;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class VWorldLog2DParser implements RecordTransform {
	private static final Logger s_logger = Logger.getLogger(VWorldLog2DParser.class);
	public static final RecordSchema SCHEMA = RecordSchema.builder()
													.addColumn("req_host", DataType.STRING)
													.addColumn("ts", DataType.DATETIME)
													.addColumn("map_type", DataType.STRING)
													.addColumn("tile", DataType.TILE)
													.addColumn("layer_name", DataType.STRING)
													.addColumn("api_Key", DataType.STRING)
													.addColumn("response_code", DataType.INT)
													.build();
	
	public VWorldLog2DParser() { }
	
	@Override
	public RecordSchema getRecordSchema() {
		return SCHEMA;
	}

	@Override
	public void setInputRecordSchema(RecordSchema schema) { }

	@Override
	public boolean transform(Record in, Record out) {
		String text = in.getString("text");
		
		LogRecord2D log = null;
		try {
			if ( text.contains("GET /2d/") ) {
				log = LogRecord2D.parse(text);
				if ( log.m_tile == null ) {
					return false;
				}
			}
			else if ( text.contains("GET /XDServer/request") ) {
				log = LogRecord2D.parseRequestLayer(text);
			}
			else {
				return false;
			}
		}
		catch ( Throwable e ) {
			s_logger.warn("fails to parse input text: \"" + text + "\", cause=" + e.getMessage());
			return false;
		}
		
		out.set(0, log.m_reqHost)
			.set(1, log.m_ts)
			.set(2, log.m_mapType)
			.set(3, log.m_tile)
			.set(4, log.m_layerName)
			.set(5, log.m_apiKey)
			.set(6, log.m_reponseCode);
		
		return true;
	}
	
	@Override
	public String toString() {
		return getClass().getName();
	}
}
