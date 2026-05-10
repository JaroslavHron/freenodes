all: freenodes

freenodes: freenodes.d
	dmd -O -inline $< -L-L/usr/local/slurm/lib -L-rpath=/usr/local/slurm/lib -L-lslurm
