SXRD_FILES := SXrdClasses.h SXrdClasses_LinkDef.h

all: playback

${SXRD_FILES}:
	svn export https://svn.gled.org/gled/trunk/demos/XrdMon/$@

SXrdClasses_Dict.cxx: ${SXRD_FILES}
	rootcint -f SXrdClasses_Dict.cxx -c -p ${SXRD_FILES}

playback: playback.cxx SXrdClasses_Dict.cxx
	g++ -o playback --std=c++11 -fPIC `root-config --cflags --libs` SXrdClasses_Dict.cxx playback.cxx
