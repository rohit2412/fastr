#! /bin/bash
java -classpath bin:lib/antlr-runtime-3.5.jar:lib/arpack_combined_all.jar:lib/junit-4.8.jar:lib/netlib-java-0.9.3.jar:lib/truffle-api-28-Jun-13.jar:lib/jline-2.12.jar:lib/h2o.jar -ea -esa r.Console $*
