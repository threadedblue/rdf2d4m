package iox.rdf2d4m;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import iox.accumulo.d4m.AccumuloInsert;

public class RDF2D4MMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static final Logger log = LoggerFactory.getLogger(RDF2D4MMapper.class);

	private static RDFFormat rdfFormat = RDFFormat.NTRIPLES;

	AccumuloInsert accIns;
	enum RDF {SUBJECT, PREDICATE, OBJECT};

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
		Boolean overwrite = new Boolean(cfg.get(RDF2D4MDriver.OVERWRITE));
		URL credsFile = new URL(cfg.get(RDF2D4MDriver.ACCUMULO_CREDS_FILE));
		accIns = new AccumuloInsert(zookeeperURI, accumuloInstance, tableName, credsFile);
		log.debug("AccumuloInsert=" + accIns);
		log.trace("<==setup");
	}

	@Override
	protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		// receives a RyaStatementWritable; convert to a Statement
		log.trace("map==>0");
		if (value.getLength() == 0) {
			return;
		}
		String s = value.toString().replaceAll("[<>]", "");
//		String s1 = value.toString().replaceAll("$ \\.", "");
		log.trace("map==>1");
		
		String[] spo = s.split(" ");
		try {
			accIns.doProcessing(spo[RDF.SUBJECT.ordinal()] + "\t", spo[RDF.PREDICATE.ordinal()] + "\t", spo[RDF.OBJECT.ordinal()] + "\t", "", "PUBLIC");
		} catch (Exception e) {
			log.error("s=" + s);
			log.error("spo=" + spo);
			log.error("" , e);
		}
		log.trace("inserted=" + spo);
		log.trace("<==map");
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	
}
