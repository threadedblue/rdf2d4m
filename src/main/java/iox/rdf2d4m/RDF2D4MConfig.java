package iox.rdf2d4m;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RDF2D4MConfig {

	private static final Logger log = LoggerFactory.getLogger(RDF2D4MConfig.class);
	
	private Map<String, String> args;
	
	private String accumuloCreds;

	public RDF2D4MConfig() {
		super();
	}

	public Map<String, String> getArgs() {
		return args;
	}

	public void setArgs(Map<String, String> args) {
		this.args = args;
	}

	public String[] getArgsAsArray() {
		log.debug("args=" + args);
		List<String> list = new ArrayList<String>();
		for (Map.Entry<String, String> entry : args.entrySet()) {
			list.add("-" + entry.getKey());
			if(entry.getValue() != null) {
				list.add(entry.getValue());
			}
		}
		return list.toArray(new String[list.size()]);
	}

	public String getAccumuloCreds() {
		return accumuloCreds;
	}
}
