package datomic.samples;

import static datomic.Peer.q;
import static datomic.Util.list;
import static datomic.Util.read;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.Fns.solo;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import datomic.Connection;
import datomic.Database;
import datomic.Entity;
import datomic.Peer;
import datomic.samples.Schema.Element.Handler;

public class Schema {
	public static abstract class Element {
		public static class Handler {
			public Handler() {	
			}

			public Object toTxMapOrList(final Element element) {
				final Comment comment = element.comment();
				if (comment != null) {
					return null;
				}
				final Partition partition = element.partition();
				if (partition != null) {
					return partition.toMap();
				}
				final Attrib attrib = element.attrib();
				if (attrib != null) {
					return attrib.toMap();
				}
				final EnumEntity enumEntity = element.enumEntity();
				if (enumEntity != null) {
					return enumEntity.toList();
				}
				throw new RuntimeException("Unknown element class: " + element.getClass().getName());
			}

			public String toEdn(final Element element) {
				final Comment comment = element.comment();
				if (comment != null) {
					return ensureEndsWithLinefeed(comment.getText());
				}
				final Partition partition = element.partition();
				if (partition != null) {
					return ensureEndsWithLinefeed(partition.toEdnMap());
				}
				final Attrib attrib = element.attrib();
				if (attrib != null) {
					return ensureEndsWithLinefeed(attrib.toEdnMap());
				}
				final EnumEntity enumEntity = element.enumEntity();
				if (enumEntity != null) {
					return ensureEndsWithLinefeed(enumEntity.toEdnList());
				}
				throw new RuntimeException("Unknown element class: " + element.getClass().getName());
			}

			private static String ensureEndsWithLinefeed(final String text) {
				if (text.endsWith("\n")) {
					return text;
				}
				return text + "\n";
			}
		}
		
		public Comment comment() {
			return null;
		}
		
		public Partition partition() {
			return null;
		}
		
		public EnumEntity enumEntity() {
			return null;
		}
		
		public Attrib attrib() {
			return null;
		}
	}	

	public static class Comment extends Element {
		public Comment(final String text) {
			this.text = text;
		}

		@Override
		public String toString() {
			return text + "\n";
		}
		
		@Override
		public Comment comment() {
			return this;			
		}

		public String getText() {
			return this.text;
		}

		private final String text;
	}
	
	public static class Partition extends Element {
		public Partition(final String ident) {
			if (ident == null || ident.length() == 0) {
				throw new IllegalArgumentException("Partition ident must be non-empty: " + ident);
			}
			if (!ident.equals(":db.part/db") && !ident.equals("db.part/db") && 
				!ident.equals(":db.part/user") && !ident.equals("db.part/user") &&
				!ident.equals(":db.part/tx") && !ident.equals("db.part/tx") &&
				(ident.startsWith(":db.") || ident.startsWith(":db/") || ident.startsWith("db.") || ident.startsWith("db/"))) {
					throw new IllegalArgumentException("Partition ident must not reside in or below the db namespace except for :db.part/{db,user,tx}: " +
						ident);
			}
			this.ident = ident;
		}

		@Override
		public Partition partition() {
			return this;
		}

		@Override
		public String toString() {
			return toEdnList();
		}

		public Map<String, Object> toMap() {
			final Map<String, Object> retval = new HashMap<String, Object>();
			retval.put(":db/id", Peer.tempid(":db.part/db"));
			retval.put(":db/ident", getIdent());
			retval.put(":db.install/_partition", ":db.part/db");
			return retval;
		}

		public String toEdnMap() {
			final StringBuffer sb = new StringBuffer();
			sb.append("{:db/id #db/id[:db.part/db]");
			sb.append("\n :db/ident ").append(getIdent());
			sb.append("\n :db.install/_partition :db.part/db}\n");
			return sb.toString();
		}

		@SuppressWarnings("unchecked")
		public List<Object> toList() {
			return list(":db/add", Peer.tempid(":db.part/db"), ":db/ident", getIdent(), ":db.install/_partition", ":db.part/db");
		}

		public String toEdnList() {
			final StringBuffer sb = new StringBuffer();
			sb.append("[:db/add #db/id[:db.part/db] :db/ident ").append(getIdent()).append(" :db.install/_partition :db.part/db]\n");
			return sb.toString();
		}

		public boolean exists(final Database db) {
			return Schema.hasPartition(db, getIdent());
		}
		
		public EnumEntity enumEntity(final String ident) {
			return new EnumEntity(getIdent(), ident);
		}

		public String getIdent() {
			return this.ident;
		}

		private final String ident;
	}

	public static class EnumEntity extends Element {
		public EnumEntity(final String ident) {
			this(":db.part/user", ident);
		}
		
		public EnumEntity(final String partitionName, final String ident) {
			if (partitionName == null || partitionName.length() == 0 || ident == null || ident.length() == 0) {
				throw new IllegalArgumentException("partitionName and ident must be non-empty");
			}
			if (!partitionName.equals(":db.part/user") && !partitionName.equals("db.part/user") &&
				(partitionName.startsWith(":db.") || partitionName.startsWith(":db/") ||
				 partitionName.startsWith("db.") || partitionName.startsWith("db/"))) {
				throw new IllegalArgumentException("Aside from :db.part/user, enum entity partition must not reside in or below the db namespace: " +
						 partitionName);
			}
			if (ident.startsWith(":db.") || ident.startsWith(":db/") || ident.startsWith("db.") || ident.startsWith("db/")) {
				throw new IllegalArgumentException("Enum entity ident must not reside in or below the db namespace: " + ident);
			}
			this.partitionName = partitionName;
			this.ident = ident;
		}

		@Override
		public EnumEntity enumEntity() {
			return this;
		}

		@Override
		public String toString() {
			return toEdnList();
		}

		public Map<String, Object> toMap() {
			final Map<String, Object> retval = new HashMap<String, Object>();
			retval.put(":db/id", Peer.tempid(getPartitionName()));
			retval.put(":db/ident", getIdent());
			return retval;
		}

		public String toEdnMap() {
			final StringBuffer sb = new StringBuffer();
			sb.append("{:db/id #db/id[").append(getPartitionName()).append("]");
			sb.append("\n :db/ident ").append(getIdent()).append("}\n");
			return sb.toString();
		}

		@SuppressWarnings("unchecked")
		public List<Object> toList() {
			return list(":db/add", Peer.tempid(getPartitionName()), ":db/ident", getIdent());
		}
		
		public String toEdnList() {
			final StringBuffer sb = new StringBuffer();
			sb.append("[:db/add #db/id[").append(getPartitionName()).append("] :db/ident ").append(getIdent()).append("]\n");
			return sb.toString();
		}

		public boolean exists(final Database db) {
			return Schema.hasEnumEntity(db, getIdent());
		}
		
		public String getPartitionName() {
			return this.partitionName;
		}
		
		public String getIdent() {
			return this.ident;
		}

		private final String partitionName;
		private final String ident;
	}

	public static class Attrib extends Element {
		public Attrib(final String ident, final String valueType, final String cardinality) {
			if (ident == null || ident.length() == 0
					|| valueType == null || valueType.length() == 0
					|| cardinality == null || cardinality.length() == 0) {
				throw new IllegalArgumentException("ident, valueType, and cardinality must all be non-empty: "
						+ ident + ", " + valueType + ", " + cardinality);
			}
			if (ident.startsWith(":db.") || ident.startsWith(":db/") || ident.startsWith("db.") || ident.startsWith("db/")) {
				throw new IllegalArgumentException("ident must not reside in or below the db namespace: " + ident);
			}
			if (!valueType.startsWith(":db.type/")) {
				throw new IllegalArgumentException("valueType must reside in the :db.type namespace: " + valueType);
			}
			if (!cardinality.equals(DB_CARDINALITY_ONE) && !cardinality.equals(DB_CARDINALITY_MANY)) {
				throw new IllegalArgumentException("cardinality must be '" + DB_CARDINALITY_ONE + "' or '" +
						DB_CARDINALITY_MANY + "': " + cardinality);
			}
			this.ident = ident;
			this.valueType = valueType;
			this.cardinality = cardinality;
		}
		
		@Override
		public Attrib attrib() {
			return this;
		}

		@Override
		public String toString() {
			return toEdnMap();
		}

		public Map<String, Object> toMap() {
			final Map<String, Object> retval = new HashMap<String, Object>();
			retval.put(":db/id", Peer.tempid(":db.part/db"));
			retval.put(":db/ident", getIdent());
			retval.put(":db/valueType", getValueType());
			retval.put(":db/cardinality", getCardinality());
			if (getComponent() != null) {
				retval.put(":db/isComponent", getComponent());
			}
			if (getDoc() != null) {
				retval.put(":db/doc", getDoc());
			}
			if (getUnique() != null) {
				retval.put(":db/unique", getUnique());
			}
			if (getIndex() != null) {
				retval.put(":db/index", getIndex());
			}
			if (getFulltext() != null) {
				retval.put(":db/fulltext", getFulltext());
			}
			if (getNoHistory() != null) {
				retval.put(":db/noHistory", getNoHistory());
			}
			retval.put(":db.install/_attribute", ":db.part/db");
			return retval;
		}

		public String toEdnMap() {
			final StringBuffer sb = new StringBuffer();
			sb.append("{:db/id #db/id[:db.part/db]");
			sb.append("\n :db/ident ").append(getIdent());
			sb.append("\n :db/valueType ").append(getValueType());
			sb.append("\n :db/cardinality ").append(getCardinality());
			if (getComponent() != null) {
				sb.append("\n :db/isComponent ").append(getComponent());
			}
			if (getDoc() != null) {
				sb.append("\n :db/doc ").append(getDoc());
			}
			if (getUnique() != null) {
				sb.append("\n :db/unique ").append(getUnique());
			}
			if (getIndex() != null) {
				sb.append("\n :db/index ").append(getIndex());
			}
			if (getFulltext() != null) {
				sb.append("\n :db/fulltext ").append(getFulltext());
			}
			if (getNoHistory() != null) {
				sb.append("\n :db/noHistory ").append(getNoHistory());
			}
			sb.append("\n :db.install/_attribute :db.part/db}\n");
			return sb.toString();
		}

		@SuppressWarnings("unchecked")
		public List<Object> toList() {
			final List<Object> retval = new ArrayList<Object>();
			retval.addAll(list(
					":db/add", Peer.tempid(":db.part/db"),
					":db/ident", getIdent(),
					":db/valueType", getValueType(),
					":db/cardinality", getCardinality()));
			if (getComponent() != null) {
				retval.addAll(list(":db/isComponent", getComponent()));
			}
			if (getDoc() != null) {
				retval.addAll(list(":db/doc", getDoc()));
			}
			if (getUnique() != null) {
				retval.addAll(list(":db/unique", getUnique()));
			}
			if (getIndex() != null) {
				retval.addAll(list(":db/index", getIndex()));
			}
			if (getFulltext() != null) {
				retval.addAll(list(":db/fulltext", getFulltext()));
			}
			if (getNoHistory() != null) {
				retval.addAll(list(":db/noHistory", getNoHistory()));
			}
			retval.addAll(list(":db.install/_attribute", ":db.part/db"));
			return retval;
		}

		public String toEdnList() {
			final StringBuffer sb = new StringBuffer();
			sb.append("{:db/add #db/id[:db.part/db] :db/ident ").append(getIdent())
				.append(" :db/valueType ").append(getValueType())
				.append(" :db/cardinality ").append(getCardinality());
			if (getComponent() != null) {
				sb.append(" :db/isComponent ").append(getComponent());
			}
			if (getDoc() != null) {
				sb.append(" :db/doc ").append(getDoc());
			}
			if (getUnique() != null) {
				sb.append(" :db/unique ").append(getUnique());
			}
			if (getIndex() != null) {
				sb.append(" :db/index ").append(getIndex());
			}
			if (getFulltext() != null) {
				sb.append(" :db/fulltext ").append(getFulltext());
			}
			if (getNoHistory() != null) {
				sb.append(" :db/noHistory ").append(getNoHistory());
			}
			sb.append(" :db.install/_attribute :db.part/db]\n");
			return sb.toString();
		}

		public boolean exists(final Database db) {
			return Schema.hasAttribute(db, getIdent());
		}
		
		public Attrib component() {
			return component(true);
		}

		public Attrib component(final Boolean component) {
			if (component != null && component.booleanValue() && !getValueType().equals(DB_TYPE_REF)) {
				throw new IllegalStateException("Only attributes of valueType=:db.type/ref can be components");
			}
			final Attrib retval = new Attrib(this);
			retval.component = component;
			return retval;
		}

		public Attrib doc(final String doc) {
			final Attrib retval = new Attrib(this);
			retval.doc = doc;
			return retval;
		}

		public Attrib uniqueValue() {
			if ("false".equals(getIndex())) {
				throw new IllegalStateException("Cannot specify :db/unique=:db.unique/value while also explicitly specifying :db/index=false");
			}
			final Attrib retval = new Attrib(this);
			retval.unique = ":db.unique/value";
			return retval;
		}

		public Attrib uniqueIdentity() {
			if ("false".equals(getIndex())) {
				throw new IllegalStateException("Cannot specify :db/unique=:db.unique/identity while also explicitly specifying :db/index=false");
			}
			final Attrib retval = new Attrib(this);
			retval.unique = ":db.unique/identity";
			return retval;
		}

		public Attrib index() {
			return index(true);
		}

		public Attrib index(final Boolean index) {
			if (index != null && !index.booleanValue() && getUnique() != null) {
				throw new IllegalStateException("Cannot explicitly specify :db/index=false while also specifying :db/unique="
						+ getUnique());
			}
			final Attrib retval = new Attrib(this);
			retval.index = index;
			return retval;
		}

		public Attrib fulltext() {
			return fulltext(true);
		}

		public Attrib fulltext(final Boolean fulltext) {
			if (fulltext != null && fulltext.booleanValue() && !getValueType().equals(DB_TYPE_STRING)) {
				throw new IllegalStateException("Cannot specify :db/fulltext=true unless db/valueType=:db.type/string: db/valueType="
						+ getValueType());
			}
			final Attrib retval = new Attrib(this);
			retval.fulltext = fulltext;
			return retval;
		}

		public Attrib noHistory() {
			return noHistory(true);
		}

		public Attrib noHistory(final Boolean noHistory) {
			final Attrib retval = new Attrib(this);
			retval.noHistory = noHistory;
			return retval;
		}

		private Attrib(final Attrib from) {
			this.ident = from.ident;
			this.valueType = from.valueType;
			this.cardinality = from.cardinality;
			this.component = from.component;
			this.doc = from.doc;
			this.unique = from.unique;
			this.index = from.index;
			this.fulltext = from.fulltext;
			this.noHistory = from.noHistory;
		}

		public String getIdent() {
			return this.ident;
		}

		public String getValueType() {
			return this.valueType;
		}

		public String getCardinality() {
			return this.cardinality;
		}

		public Boolean getComponent() {
			return this.component;
		}

		public String getDoc() {
			return this.doc;
		}

		public String getUnique() {
			return this.unique;
		}

		public Boolean getIndex() {
			return this.index;
		}

		public Boolean getFulltext() {
			return this.fulltext;
		}

		public Boolean getNoHistory() {
			return this.noHistory;
		}

		private final String ident;
		private final String valueType;
		private final String cardinality;
		private Boolean component;
		private String doc = null;
		private String unique = null;
		private Boolean index;
		private Boolean fulltext = null;
		private Boolean noHistory = null;
	}

	public static Comment comment(final String text) {
		return new Comment(text);
	}

	public static Attrib keywordAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_KEYWORD, DB_CARDINALITY_ONE);
    }

    public static Attrib manyKeywordsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_KEYWORD, DB_CARDINALITY_MANY);
    }

    public static Attrib stringAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_STRING, DB_CARDINALITY_ONE);
    }

    public static Attrib manyStringsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_STRING, DB_CARDINALITY_MANY);
    }

    public static Attrib booleanAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BOOLEAN, DB_CARDINALITY_ONE);
    }

    public static Attrib manyBooleansAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BOOLEAN, DB_CARDINALITY_MANY);
    }

    public static Attrib longAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_LONG, DB_CARDINALITY_ONE);
    }

    public static Attrib manyLongsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_LONG, DB_CARDINALITY_MANY);
    }

    public static Attrib bigintAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BIGINT, DB_CARDINALITY_ONE);
    }

    public static Attrib manyBigintsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BIGINT, DB_CARDINALITY_MANY);
    }

    public static Attrib floatAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_FLOAT, DB_CARDINALITY_ONE);
    }

    public static Attrib manyFloatsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_FLOAT, DB_CARDINALITY_MANY);
    }

    public static Attrib doubleAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_DOUBLE, DB_CARDINALITY_ONE);
    }

    public static Attrib manyDoublesAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_DOUBLE, DB_CARDINALITY_MANY);
    }

    public static Attrib bigdecAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BIGDEC, DB_CARDINALITY_ONE);
    }

    public static Attrib manyBigdecsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BIGDEC, DB_CARDINALITY_MANY);
    }

    public static Attrib refAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_REF, DB_CARDINALITY_ONE);
    }

    public static Attrib refComponentAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_REF, DB_CARDINALITY_ONE).component();
    }

    public static Attrib manyRefsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_REF, DB_CARDINALITY_MANY);
    }

    public static Attrib manyRefComponentsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_REF, DB_CARDINALITY_MANY).component();
    }

    public static Attrib instantAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_INSTANT, DB_CARDINALITY_ONE);
    }

    public static Attrib manyInstantsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_INSTANT, DB_CARDINALITY_MANY);
    }

    public static Attrib uuidAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_UUID, DB_CARDINALITY_ONE);
    }

    public static Attrib manuUuidsAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_UUID, DB_CARDINALITY_MANY);
    }

    public static Attrib uriAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_URI, DB_CARDINALITY_ONE);
    }

    public static Attrib manuUrisAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_URI, DB_CARDINALITY_MANY);
    }

    public static Attrib bytesAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BYTES, DB_CARDINALITY_ONE);
    }

    public static Attrib manyBytesAttrib(final String ident) {
    	return new Attrib(ident, DB_TYPE_BYTES, DB_CARDINALITY_MANY);
    }
    
    public static EnumEntity enumEntity(final String ident) {
    	return new EnumEntity(ident);
    }

    public static EnumEntity enumEntity(final String partitionName, final String ident) {
    	return new EnumEntity(partitionName, ident);
    }

    public static Object cardinality(Object db, Object attr) {
        return solo(solo(q("[:find ?v " +
                           ":in $ ?attr " +
                           ":where " +
                           "[?attr :db/cardinality ?card] " +
                           "[?card :db/ident ?v]]", db, attr)));
    }

    public static boolean hasAttribute(Object db, String attrName) {
    	Entity entity = ((Database)db).entity(attrName);
		return entity.get(":db.install/_attribute") != null;
    }
    
    public static boolean hasPartition(Object db, String partitionName) {
    	// the linkage for db.part/user and db.part/tx is not explicit
    	// as per https://groups.google.com/d/msg/datomic/9xxoWH6RxhA/AYVObmrhBp0J
    	if (":db.part/user".equals(partitionName) || "db.part/user".equals(partitionName) ||
    		":db.part/tx".equals(partitionName) || "db.part/tx".equals(partitionName)) {
    		return true;
    	}
    	return !q("[:find ?p " +
                   ":in $ ?partitionName " +
    			   ":where [_ :db.install/partition ?p] [?p :db/ident ?partitionName]]",
    			   db, partitionName)
    			.isEmpty();
    }
    
    public static boolean hasEnumEntity(Object db, String ident) {
    	Entity entity = ((Database)db).entity(ident);
		return entity.get(":db/id") != null;
    }

    public static boolean hasSchema(Object db, String schemaAttrName, String schemaName) {
    	return hasAttribute(db, schemaAttrName) &&
    		!q("[:find ?e " +
                ":in $ ?sa ?sn " +
                ":where [?e ?sa ?sn]]", db, schemaAttrName, schemaName).isEmpty();        
    }    

    public static void ensurePartition(final Connection conn, final String partitionName) {
    	if (!hasPartition(conn.db(), partitionName)) {
    		try {
				conn.transact(list(new Partition(partitionName).toMap())).get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
    	}
    }

    public static void transact(final Connection conn, final List<List<Element>> elementTxs) throws InterruptedException, ExecutionException {
    	transact(conn, elementTxs, new Handler());
    }

    public static void transact(final Connection conn, final List<List<Element>> elementTxs, final Handler handler) throws InterruptedException, ExecutionException {
    	if (handler == null) {
    		transact(conn, elementTxs, new Handler());
    		return;
    	}
    	for (List<Element> elementTx : elementTxs) {
    		final List<Object> tx = new ArrayList<Object>(elementTx.size());
    		for (Element element : elementTx) {
        		final Object txMapOrList = handler.toTxMapOrList(element);
        		if (txMapOrList != null) {
        			tx.add(txMapOrList);
        		}
			}
    		conn.transact(tx).get();
		}
    }

    public static String toEdn(final List<List<Element>> elementTxs) throws InterruptedException, ExecutionException {
    	return toEdn(elementTxs, new Handler());
    }

    public static String toEdn(final List<List<Element>> elementTxs, final Handler handler) throws InterruptedException, ExecutionException {
    	if (handler == null) {
    		return toEdn (elementTxs, new Handler());
    	}
    	final StringBuffer sb = new StringBuffer();
    	boolean first = true;
    	for (List<Element> elementTx : elementTxs) {
    		sb.append(first ? "[\n" : "\n[\n");
    		first = false;
    		for (Element element : elementTx) {
        		final String edn = handler.toEdn(element);
        		sb.append(edn);
			}
    		sb.append("]");
		}
    	return sb.toString();
    }

    private static void ensureSchemaAttribute(Object conn, String schemaAttrName) {
    	if (!hasAttribute(((Connection)conn).db(), schemaAttrName)) {
    		try {
				((Connection)conn).transact(list(keywordAttrib(schemaAttrName)
						.doc("Name of schema installed by this transaction")
						.index().toMap())).get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
    	}
    }

	private static final String DB_CARDINALITY_MANY = ":db.cardinality/many";
	private static final String DB_CARDINALITY_ONE = ":db.cardinality/one";
	public static final Object CARDINALITY_ONE = read(DB_CARDINALITY_ONE);
    public static final Object CARDINALITY_MANY = read(DB_CARDINALITY_MANY);
	private static final String DB_TYPE_BYTES = ":db.type/bytes";
	private static final String DB_TYPE_URI = ":db.type/uri";
	private static final String DB_TYPE_UUID = ":db.type/uuid";
	private static final String DB_TYPE_INSTANT = ":db.type/instant";
	private static final String DB_TYPE_REF = ":db.type/ref";
	private static final String DB_TYPE_BIGDEC = ":db.type/bigdec";
	private static final String DB_TYPE_DOUBLE = ":db.type/double";
	private static final String DB_TYPE_FLOAT = ":db.type/float";
	private static final String DB_TYPE_BIGINT = ":db.type/bigint";
	private static final String DB_TYPE_LONG = ":db.type/long";
	private static final String DB_TYPE_BOOLEAN = ":db.type/boolean";
	private static final String DB_TYPE_STRING = ":db.type/string";
	private static final String DB_TYPE_KEYWORD = ":db.type/keyword";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Connection conn = scratchConnection();
        Database db = conn.db();

        System.out.println("Database has a :db.part/db partition?");
        System.out.println(hasPartition(db, ":db.part/db"));
        System.out.println("Database has a :db.part/user partition?");
        System.out.println(hasPartition(db, ":db.part/user"));
        System.out.println("Database has a :db.part/tx partition?");
        System.out.println(hasPartition(db, ":db.part/tx"));
        System.out.println("Database has a :foo partition?");
        System.out.println(hasPartition(db, ":foo"));
        System.out.println("Ensure that there is a :foo partition");
        ensurePartition(conn, ":foo");
        db = conn.db(); // get latest version of the database so we see the change
        System.out.println("Database has a :foo partition now?");
        System.out.println(hasPartition(db, ":foo"));
        System.out.println("Ensure that there is a :foo partition -- idempotent");
        ensurePartition(conn, ":foo");
        db = conn.db(); // get latest version of the database so we see the change
        System.out.println("Database has a :foo partition now?");
        System.out.println(hasPartition(db, ":foo"));

        // generate the seattle schema
        final List<Element> elements =  new ArrayList<Element>();
        //
        elements.add(comment(";; community"));
        elements.add(stringAttrib(":community/name").fulltext().doc("A community's name"));
        elements.add(stringAttrib(":community/url").doc("A community's url"));
        elements.add(refAttrib(":community/neighborhood").doc("A community's neighborhood"));
        elements.add(manyStringsAttrib(":community/category").fulltext().doc("All community categories"));
        elements.add(refAttrib(":community/orgtype").doc("A community orgtype enum value"));
        elements.add(refAttrib(":community/type").doc("A community type enum value"));
        //
        elements.add(comment(";; community/orgtype enum values"));
        elements.add(enumEntity(":community.orgtype/community"));
        elements.add(enumEntity(":community.orgtype/commercial"));
        elements.add(enumEntity(":community.orgtype/nonprofit"));
        elements.add(enumEntity(":community.orgtype/personal"));
        //
        elements.add(comment(";; community/type enum values"));
        elements.add(enumEntity(":community.type/email-list"));
        elements.add(enumEntity(":community.type/twitter"));
        elements.add(enumEntity(":community.type/facebook-page"));
        elements.add(enumEntity(":community.type/blog"));
        elements.add(enumEntity(":community.type/website"));
        elements.add(enumEntity(":community.type/wiki"));
        elements.add(enumEntity(":community.type/myspace"));
        elements.add(enumEntity(":community.type/ning"));
        //
        elements.add(comment(";; neighborhood"));
        elements.add(stringAttrib(":neighborhood/name").uniqueIdentity().doc("A unique neighborhood name (upsertable)"));
        elements.add(refAttrib(":neighborhood/district").uniqueIdentity().doc("A neighborhood's district"));
        //
        elements.add(comment(";; district"));
        elements.add(stringAttrib(":district/name").uniqueIdentity().doc("A unique district name (upsertable)"));
        elements.add(refAttrib(":district/region").doc("A district region enum value"));
        //
        elements.add(comment(";; district/region enum values"));
        elements.add(enumEntity(":region/n"));
        elements.add(enumEntity(":region/ne"));
        elements.add(enumEntity(":region/e"));
        elements.add(enumEntity(":region/se"));
        elements.add(enumEntity(":region/s"));
        elements.add(enumEntity(":region/sw"));
        elements.add(enumEntity(":region/w"));
        elements.add(enumEntity(":region/nw"));
        System.out.println("Here is what the list of elements looks like when you invoke toString():");
        System.out.println(elements.toString());
        final String edn = Schema.toEdn(list(elements));
        System.out.println("Here is what the list of elements looks like when you convert it to edn:");
		System.out.println(edn);
        Schema.transact(conn, list(elements));
        // do it again to make sure the edn is valid
        IO.transactAll(conn, new StringReader(edn));
        db = conn.db(); // get latest version of the database so we see the change
        System.out.println("Database has a :community/name attribute?");
        System.out.println(hasAttribute(db, ":community/name") ?
        	"true; cardinality = " + cardinality(db, ":community/name") : false);
        System.out.println("Database has a :community/category attribute?");
        System.out.println(hasAttribute(db, ":community/category") ?
        	"true; cardinality = " + cardinality(db, ":community/category") : false);
        System.out.println("Database has a :bar attribute?");
        System.out.println(hasAttribute(db, ":bar") ?
        	"true; cardinality = " + cardinality(db, ":bar") : false);
        System.out.println("Database has a :community.orgtype/community enum entity?");
        System.out.println(hasEnumEntity(db, ":community.orgtype/community"));
        System.out.println("Database has a :foobar enum entity?");
        System.out.println(hasEnumEntity(db, ":foobar"));
        System.exit(0);
    }
}
