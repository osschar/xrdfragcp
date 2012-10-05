xrdloc := /usr

xrdfragcp: xrdfragcp.cxx
	${CXX} -o $@ -I${xrdloc}/include/xrootd -L${xrdloc}/lib64 -lXrdClient -lpcrecpp -pthread $<

hdtools.jar: hdtools/HadoopTools.java
	javac -classpath '/usr/lib/hadoop-0.20/*:/usr/lib/hadoop-0.20/lib/*' $<
	jar cvfm $@ hdtools/Manifest.txt hdtools

clean:
	rm -rf xrdfragcp
