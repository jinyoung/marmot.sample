package vworld;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotRuntime;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordTransform;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class VWorldLog2DParser implements RecordTransform, Serializable {
	private static final long serialVersionUID = -3948145055174680350L;

	private static final Logger s_logger = LoggerFactory.getLogger(VWorldLog2DParser.class);
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
	public RecordSchema getOutputRecordSchema() {
		return SCHEMA;
	}

	@Override
	public void setInputRecordSchema(MarmotRuntime marmot, RecordSchema schema) { }

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
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		
	}
	private void writeObject(ObjectOutputStream oos) throws IOException {
		
	}
}
