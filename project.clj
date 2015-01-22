(defproject ru.prepor/clj-kafka "0.3.1"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 ;; conflicts
                 [org.clojure/data.json "0.2.3"]

                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-kafka "0.2.8-0.8.1.1"
                  :exclusions [[log4j]
                               [org.slf4j/slf4j-simple]]]
                 [org.apache.commons/commons-pool2 "2.2"]
                 [org.apache.commons/commons-io "1.3.2"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.stuartsierra/component "0.2.2"]
                 [com.taoensso/carmine "2.4.4"]
                 [ru.prepor/utils "0.2.0"]
                 [org.apache.zookeeper/zookeeper "3.4.6"
                  :exclusions [[com.sun.jmx/jmxri]
                               [com.sun.jdmk/jmxtools]
                               [javax.jms/jms]
                               [javax.mail/mail]
                               [org.slf4j/slf4j-log4j12]
                               [org.slf4j/slf4j-simple]
                               [log4j]]]
                 [org.apache.curator/curator-recipes "2.7.0"]
                 [org.apache.curator/curator-test "2.7.0"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-api "1.7.7"]
                                  [ch.qos.logback/logback-classic "1.1.2"]
                                  [org.slf4j/log4j-over-slf4j "1.7.7"]]}}
  :lein-release {:deploy-via :clojars})
