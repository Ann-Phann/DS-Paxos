#=============================
# make test NUM=1   # runs Test1
# make test NUM=2   # runs Test2
# make test NUM=3   # runs Test3
#=============================

# Default target
.PHONY: test compile

# Compile the project
compile:
	@echo "Compiling project..."
	@mvn compile
	@echo "Compilation complete."

# Usage: make test NUM=1
test: compile
ifeq ($(NUM),)
	$(error Please provide a test number, e.g., make test NUM=1)
endif
	mvn exec:java -Dexec.mainClass=com.an.paxos.Test$(NUM)
