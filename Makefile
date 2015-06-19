all: freenodes showqueue

freenodes: freenodes.d
	dmd -O -inline $<
showqueue: showqueue.d
	dmd -O -inline $<
