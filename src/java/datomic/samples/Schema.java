package datomic.samples;

import static datomic.Peer.q;
import static datomic.Util.list;
import static datomic.Util.read;
import static datomic.samples.Fns.scratchConnection;
import static datomic.samples.Fns.solo;
import static datomic.samples.IO.transactAllFromResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import datomic.Connection;
import datomic.Database;
import datomic.Entity;
import datomic.Peer;

public class Schema {
	public static class Partition {
		public Partition(final String ident) {
			if (ident == null || ident.length() == 0) {
				throw new IllegalArgumentException("ident must be non-empty: " + ident);
			}
			if (ident.startsWith(":db.") || ident.startsWith(":db/")) {
				throw new IllegalArgumentException("ident for a user-defined partition must not reside in or below the db namespace: " +
						ident);
			}
			this.ident = ident;
		}

		@Override
		public String toString() {
			return toMap().toString();
		}

		public Map<String, Object> toMap() {
			final Map<String, Object> retval = new HashMap<String, Object>();
			retval.put(":db/id", Peer.tempid(":db.part/db"));
			retval.put(":db/ident", ident);
			retval.put(":db.install/_partition", ":db.part/db");
			return retval;
		}

		public EnumEntity enumEntity(final String enumIdent) {
			return new EnumEntity(ident, enumIdent);
		}
		
		private final String ident;
	}

	public static class EnumEntity {
		public EnumEntity(final String ident) {
			this ("db.part/user", ident);
		}
		
		public EnumEntity(final String partitionName, final String ident) {
			if (partitionName == null || partitionName.length() == 0 || ident == null || ident.length() == 0) {
				throw new IllegalArgumentException("partitionName and ident must be non-empty");
			}
			if (ident.startsWith(":db.") || ident.startsWith(":db/")) {
				throw new IllegalArgumentException("ident must not reside in or below the db namespace: " + ident);
			}
			this.partitionName = partitionName;
			this.ident = ident;
		}

		@Override
		public String toString() {
			return toList().toString();
		}

		public List<Object> toList() {
			return list(":db/add",
					    Peer.tempid(partitionName),
					    ":db/ident", ident);
		}
		
		private final String partitionName;
		private final String ident;
	}

	public static class Attrib {
		public Attrib(final String ident, final String valueType, final String cardinality) {
			if (ident == null || ident.length() == 0
					|| valueType == null || valueType.length() == 0
					|| cardinality == null || cardinality.length() == 0) {
				throw new IllegalArgumentException("ident, valueType, and cardinality must all be non-empty: "
						+ ident + ", " + valueType + ", " + cardinality);
			}
			if (!ident.startsWith(":") || ident.length() == 1) {
				throw new IllegalArgumentException("ident must be a keyword, meaning it must start with a ':' and at least one additional character: "
						+ ident);
			}
			if (ident.startsWith(":db.") || ident.startsWith(":db/")) {
				throw new IllegalArgumentException("ident must not reside in or below the db namespace: " + ident);
			}
			if (!valueType.startsWith(":db.type/")) {
				throw new IllegalArgumentException("valueType must reside in the db.type namespace: " + valueType);
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
		public String toString() {
			return toMap().toString();
		}

		public Map<String, Object> toMap() {
			final Map<String, Object> retval = new HashMap<String, Object>();
			retval.put(":db/id", Peer.tempid(":db.part/db"));
			retval.put(":db/ident", ident);
			retval.put(":db/valueType", valueType);
			retval.put(":db/cardinality", cardinality);
			if (component != null) {
				retval.put(":db/isComponent", component);
			}
			if (doc != null) {
				retval.put(":db/doc", doc);
			}
			if (unique != null) {
				retval.put(":db/unique", unique);
			}
			if (index != null) {
				retval.put(":db/index", index);
			}
			if (fulltext != null) {
				retval.put(":db/fulltext", fulltext);
			}
			if (noHistory != null) {
				retval.put(":db/noHistory", noHistory);
			}
			retval.put(":db.install/_attribute", ":db.part/db");
			return retval;
		}

		public Attrib component() {
			return component(true);
		}

		public Attrib component(final Boolean component) {
			if (component != null && component.booleanValue() && !valueType.equals("db.type/ref")) {
				throw new IllegalStateException("Only attributes of valueType=db.type/ref can be components");
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
			if ("false".equals(this.index)) {
				throw new IllegalStateException("Cannot specify :db/unique=:db.unique/value while also explicitly specifying :db/index=false");
			}
			final Attrib retval = new Attrib(this);
			retval.unique = ":db.unique/value";
			return retval;
		}

		public Attrib uniqueIdentity() {
			if ("false".equals(this.index)) {
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
			if (index != null && !index.booleanValue() && unique != null) {
				throw new IllegalStateException("Cannot explicitly specify :db/index=false while also specifying :db/unique="
						+ unique);
			}
			final Attrib retval = new Attrib(this);
			retval.index = index;
			return retval;
		}

		public Attrib fulltext() {
			return fulltext(true);
		}

		public Attrib fulltext(final Boolean fulltext) {
			if (fulltext != null && fulltext.booleanValue() && !valueType.equals(":db.type/string")) {
				throw new IllegalStateException("Cannot specify :db/fulltext=true unless db/valueType=db.type/string: db/valueType="
						+ valueType);
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

	private static final String DB_CARDINALITY_MANY = ":db.cardinality/many";
	private static final String DB_CARDINALITY_ONE = ":db.cardinality/one";
	public static final Object CARDINALITY_ONE = read(DB_CARDINALITY_ONE);
    public static final Object CARDINALITY_MANY = read(DB_CARDINALITY_MANY);

    public static Attrib keywordAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/keyword", DB_CARDINALITY_ONE);
    }

    public static Attrib manyKeywordsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/keyword", DB_CARDINALITY_MANY);
    }

    public static Attrib stringAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/string", DB_CARDINALITY_ONE);
    }

    public static Attrib manyStringsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/string", DB_CARDINALITY_MANY);
    }

    public static Attrib booleanAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/boolean", DB_CARDINALITY_ONE);
    }

    public static Attrib manyBooleansAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/boolean", DB_CARDINALITY_MANY);
    }

    public static Attrib longAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/long", DB_CARDINALITY_ONE);
    }

    public static Attrib manyLongsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/long", DB_CARDINALITY_MANY);
    }

    public static Attrib bigintAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/bigint", DB_CARDINALITY_ONE);
    }

    public static Attrib manyBigintsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/bigint", DB_CARDINALITY_MANY);
    }

    public static Attrib floatAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/float", DB_CARDINALITY_ONE);
    }

    public static Attrib manyFloatsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/float", DB_CARDINALITY_MANY);
    }

    public static Attrib doubleAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/double", DB_CARDINALITY_ONE);
    }

    public static Attrib manyDoublesAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/double", DB_CARDINALITY_MANY);
    }

    public static Attrib bigdecAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/bigdec", DB_CARDINALITY_ONE);
    }

    public static Attrib manyBigdecsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/bigdec", DB_CARDINALITY_MANY);
    }

    public static Attrib refAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/ref", DB_CARDINALITY_ONE);
    }

    public static Attrib refComponentAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/ref", DB_CARDINALITY_ONE).component();
    }

    public static Attrib manyRefsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/ref", DB_CARDINALITY_MANY);
    }

    public static Attrib manyRefComponentsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/ref", DB_CARDINALITY_MANY).component();
    }

    public static Attrib instantAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/instant", DB_CARDINALITY_ONE);
    }

    public static Attrib manyInstantsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/instant", DB_CARDINALITY_MANY);
    }

    public static Attrib uuidAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/uuid", DB_CARDINALITY_ONE);
    }

    public static Attrib manuUuidsAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/uuid", DB_CARDINALITY_MANY);
    }

    public static Attrib uriAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/uri", DB_CARDINALITY_ONE);
    }

    public static Attrib manuUrisAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/uri", DB_CARDINALITY_MANY);
    }

    public static Attrib bytesAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/bytes", DB_CARDINALITY_ONE);
    }

    public static Attrib manyBytesAttrib(final String ident) {
    	return new Attrib(ident, ":db.type/bytes", DB_CARDINALITY_MANY);
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
    	if (":db.part/user".equals(partitionName) || ":db.part/tx".equals(partitionName)) {
    		return true;
    	}
    	return !q("[:find ?p " +
                   ":in $ ?partitionName " +
    			   ":where [_ :db.install/partition ?p] [?p :db/ident ?partitionName]]",
    			   db, partitionName)
    			.isEmpty();
    }
    
    public static boolean hasSchema(Object db, String schemaAttrName, String schemaName) {
    	return hasAttribute(db, schemaAttrName) &&
    		!q("[:find ?e " +
                ":in $ ?sa ?sn " +
                ":where [?e ?sa ?sn]]", db, schemaAttrName, schemaName).isEmpty();        
    }    

    public static void ensurePartition(Object conn, String partitionName) {
    	if (!hasPartition(((Connection)conn).db(), partitionName)) {
    		try {
				((Connection)conn).transact(list(new Partition(partitionName).toMap())).get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
    	}
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
        List<Object> tx =  new ArrayList<Object>();
        tx.add(stringAttrib(":community/name").fulltext().doc("A community's name").toMap());
        tx.add(stringAttrib(":community/url").doc("A community's url").toMap());
        tx.add(refAttrib(":community/neighborhood").doc("A community's neighborhood").toMap());
        tx.add(manyStringsAttrib(":community/category").fulltext().doc("All community categories").toMap());
        tx.add(refAttrib(":community/orgtype").doc("A community orgtype enum value").toMap());
        tx.add(refAttrib(":community/type").doc("A community type enum value").toMap());
        tx.add(enumEntity(":community.orgtype/community").toList());
        tx.add(enumEntity(":community.orgtype/commercial").toList());
        tx.add(enumEntity(":community.orgtype/nonprofit").toList());
        tx.add(enumEntity(":community.orgtype/personal").toList());
        tx.add(enumEntity(":community.type/email-list").toList());
        tx.add(enumEntity(":community.type/twitter").toList());
        tx.add(enumEntity(":community.type/facebook-page").toList());
        tx.add(enumEntity(":community.type/blog").toList());
        tx.add(enumEntity(":community.type/website").toList());
        tx.add(enumEntity(":community.type/wiki").toList());
        tx.add(enumEntity(":community.type/myspace").toList());
        tx.add(enumEntity(":community.type/ning").toList());
        tx.add(stringAttrib(":neighborhood/name").uniqueIdentity().doc("A unique neighborhood name (upsertable)").toMap());
        tx.add(refAttrib(":neighborhood/district").uniqueIdentity().doc("A neighborhood's district").toMap());
        tx.add(stringAttrib(":district/name").uniqueIdentity().doc("A unique district name (upsertable)").toMap());
        tx.add(refAttrib(":district/region").doc("A district region enum value").toMap());
        tx.add(enumEntity(":region/n").toList());
        tx.add(enumEntity(":region/ne").toList());
        tx.add(enumEntity(":region/e").toList());
        tx.add(enumEntity(":region/se").toList());
        tx.add(enumEntity(":region/s").toList());
        tx.add(enumEntity(":region/sw").toList());
        tx.add(enumEntity(":region/w").toList());
        tx.add(enumEntity(":region/nw").toList());
        System.out.println(tx.toString());
        conn.transact(tx).get();
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
        System.exit(0);
    }
}
