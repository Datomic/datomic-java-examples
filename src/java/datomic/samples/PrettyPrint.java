package datomic.samples;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class PrettyPrint {
    public static final IFn prettyPrint = Clojure.var("clojure.pprint", "pprint");
    public static void print(Object o) {
        prettyPrint.invoke(o);
    }
}
