(defproject io.cyanite/cyanite "0.1.3"
  :description "Alternative storage backend for graphite, backed by cassandra"
  :url "https://github.com/pyr/cyanite"
  :license {:name "MIT License"
            :url "https://github.com/pyr/cyanite/tree/master/LICENSE"}
  :maintainer {:email "pyr@spootnik.org"}
  :aot :all
  :main io.cyanite
  :dependencies [[org.clojure/clojure           "1.7.0-beta3"]
                 [org.clojure/tools.logging     "0.3.1"]
                 [org.clojure/tools.cli         "0.3.1"]
                 [org.spootnik/pickler          "0.1.0"]
                 [org.spootnik/logconfig        "0.7.3"]
                 [ring/ring-codec               "1.0.0"]
                 [clj-yaml                      "0.4.0"]
                 [clj-http                      "1.0.1"
                  :exclusions [commons-codec]]
                 [cc.qbits/jet                  "0.6.4"]
                 [cc.qbits/alia                 "2.5.3"]
                 [net.jpountz.lz4/lz4           "1.2.0"]
                 [org.xerial.snappy/snappy-java "1.1.1.3"]
                 [io.netty/netty-all            "4.0.28.Final"]
                 [org.clojure/core.async        "0.1.346.0-17112a-alpha"]
                 [clojurewerkz/elastisch        "2.2.0-beta3"
                  :exclusions [com.google.guava/guava commons-codec]]])
