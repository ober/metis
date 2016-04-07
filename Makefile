.DEFAULT_GOAL := usage

usage:
	@ echo "make all # create distributions for all installed lisps"


lispworks:
	@ ~/lw-console -build deliver.lisp

sbcl:
	@ mkdir -p dist/sbcl
	@ cat deliver.lisp|sbcl --dynamic-space-size 2048
ccl:
	@ make -p dist/ccl || true
	@ cat deliver.lisp|ccl64

clisp:
	@ mkdir -p dist/clisp || true
	@ cat deliver.lisp|clisp

allegro:
	@ rm -rf ./metis/
	@ cat deliver.lisp|allegro

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
