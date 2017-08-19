
grantPermissions:
	chmod 766 ../bin/*

run:
	../bin/run.sh

runA:
	../bin/run.sh -a

# This is a quick check for timming.
# For thorough results, use the profile
# option
runT:
	echo "Simple timing analysis. Use the 'profile' option for proper profiling"
	../bin/run.sh -t

profile:
	python3 -m cProfile -s cumtime theFlood.py

clean:
	../bin/clean.sh