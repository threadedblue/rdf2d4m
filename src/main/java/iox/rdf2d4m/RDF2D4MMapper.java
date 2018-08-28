package iox.rdf2d4m;

import java.io.IOException;
import java.net.URL;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.mit.ll.graphulo.util.D4MTableWriter;
import edu.mit.ll.graphulo.util.D4MTableWriter.D4MTableConfig;
import iox.accumulo.AccumuloAccess;

public class RDF2D4MMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static final Logger log = LoggerFactory.getLogger(RDF2D4MMapper.class);

	private static RDFFormat rdfFormat = RDFFormat.NTRIPLES;

	D4MTableWriter d4mTW;
	enum RDF {SUBJECT, PREDICATE, OBJECT};
	int count;

	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context ctx)
			throws IOException, InterruptedException {
		log.trace("setup==>0");
		super.setup(ctx);
		log.trace("setup==>1");

		log.trace("setup==>3");
		Configuration cfg = ctx.getConfiguration();
		String zookeeperURI  = cfg.get(RDF2D4MDriver.ZOOKEEPER_URI);
		String accumuloInstance = cfg.get(RDF2D4MDriver.ACCUMULO_INSTANCE);
		String tableName = cfg.get(RDF2D4MDriver.TABLE_NAME);
		Boolean overwrite = cfg.getBoolean(RDF2D4MDriver.OVERWRITE, false);
		URL credsFile = new URL("file:///home/haz/accumulo-creds.yml");
		AccumuloAccess db = new AccumuloAccess("haz00:2181", "accumulo", credsFile);
		D4MTableConfig tconf = new D4MTableConfig();
		tconf.baseName = "ccdSOP";
		tconf.connector = db.getConnection();
		tconf.useTable = true;
		tconf.useTableT = true;
		tconf.useTableDeg = true;
//		tconf.deleteExistingTables = true;
		d4mTW = new D4MTableWriter(tconf);
		D4MTableWriter.createTableSoft("ccdSOP", db.getConnection(), overwrite);
		
		log.debug("<==setup" + "zoo=" + zookeeperURI);
	}

	@Override
	protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
 
		log.trace("map==>0");
		if (value.getLength() == 0) {
			return;
		}
		String s = value.toString().replaceAll("[<>]", "");
		log.trace("map==>1");
		
		String[] spo = s.split(" ");
		try {
			d4mTW.ingestRow(new Text(spo[RDF.SUBJECT.ordinal()]), new Text(spo[RDF.OBJECT.ordinal()]), new Value(spo[RDF.PREDICATE.ordinal()]));
		} catch (Exception e) {
			log.error("s=" + s);
			log.error("spo=" + spo);
			log.error("" , e);
		}
		count++;
		if(count % 5000 == 0) {
			d4mTW.flushBuffers();
			log.debug("inserted=" + count);
		}
		log.trace("<==map");
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	
}
