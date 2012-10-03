xrdloc := /usr

xrdfragcp: xrdfragcp.cxx
	${CXX} -o $@ -I${xrdloc}/include/xrootd -L${xrdloc}/lib64 -lXrdClient -pthread $<

clean:
	rm -rf xrdfragcp
