.DEFAULT_GOAL := usage

usage:
	@ echo "make all # create distributions for all installed lisps"


lispworks:
	@ mkdir -p dist/lispworks
	@ cd src && lispworks -build deliver.lisp

ecl:
	@ mkdir -p dist/ecl
	@ cat collector/deliver-ecl.lisp|ecl

sbcl:
	@ mkdir -p dist/sbcl
	@ cat src/deliver.lisp|sbcl  --dynamic-space-size 20480 --control-stack-size 2048  #--disable-debugger
ccl:
	@ mkdir -p dist/ccl || true
	@ cd src && cat deliver.lisp|ccl64

clisp:
	@ mkdir -p dist/clisp || true
	@ cat deliver.lisp|clisp

allegro:
	@ rm -rf dist/allegro metis
	@ mkdir -p dist/allegro
	@ cd src && cat deliver.lisp|allegro
	@ mv src/metis/* dist/allegro
	@ rm -rf src/metis

abcl:
	@ mkdir -p dist/abcl || true
	@ cat deliver.lisp|abcl

cmucl:
	@ cat deliver.lisp|/usr/cmucl/bin/lisp

bench:
	echo "****************SBCL**********************************"
	time cat collector/run.lisp|sbcl --dynamic-space-size 2048
	echo "**************** CCL **********************************"
	time cat collector/run.lisp|ccl64
	echo "**************** ALLEGRO **********************************"
	time cat collector/run.lisp|mlisp | grep -v New
	echo "**************** LISPWORKS **********************************"
	time cat collector/run.lisp|lw-console
	echo "**************** CMUCL **********************************"
	time cat collector/run.lisp|/usr/bin/cmucl | grep -v New
	echo "**************** ABCL **********************************"
	time cat collector/run.lisp|abcl | grep -v New


all: lispworks sbcl ccl allegro
