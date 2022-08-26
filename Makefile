all: freenodes

freenodes: freenodes.d
	dmd -O -inline $<
