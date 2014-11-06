(defproject ru.prepor/clj-kafka "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
                 ;; conflicts
                 [org.clojure/data.json "0.2.3"]
                 [org.slf4j/slf4j-api "1.7.2"]
                 [log4j "1.2.17"]

                 [org.clojure/clojure "1.6.0"]
                 [com.taoensso/timbre "3.0.1"]
                 [clj-kafka "0.2.6-0.8"]
                 [org.apache.commons/commons-pool2 "2.2"]
                 [org.apache.commons/commons-io "1.3.2"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [com.stuartsierra/component "0.2.2"]
                 [com.taoensso/carmine "2.4.4"]
                 [ru.prepor/utils "0.1.0"]
                 [org.apache.zookeeper/zookeeper "3.4.0"
                  :exclusions [[com.sun.jmx/jmxri]
                               [com.sun.jdmk/jmxtools]
                               [javax.jms/jms]
                               [javax.mail/mail]]]
                 [org.apache.curator/curator-test "2.6.0"]])
