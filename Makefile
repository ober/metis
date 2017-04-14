.DEFAULT_GOAL := usage
.PHONY: chicken

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
	@ cat src/deliver.lisp|sbcl  --dynamic-space-size 20480  2>&1 > /dev/null #--control-stack-size 2048  #--disable-debugger
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

chicken:
	@ rm -rf ./dist/chicken
	@ mkdir -p dist/chicken
	@ /usr/local/bin/chicken-install z3 medea vector-lib posix files srfi-13 format list-bindings s11n srfi-19 args
	@ /usr/local/bin/chicken-install -deploy -p dist/chicken z3 medea vector-lib posix files srfi-13 format list-bindings s11n srfi-19 args
	@ /usr/local/bin/csc -deploy -o dist/chicken chicken/metis.scm
	@ mv dist/chicken/chicken dist/chicken/metis

bench: all
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" time metis-sbcl s ~/ct-test > results/sbcl 2>&1
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" time metis-lispworks s ~/ct-test > results/lispworks 2>&1
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" time metis-allegro s ~/ct-test > results/allegro  2>&1
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" time metis-ccl s ~/ct-test > results/ccl 2>&1
	rm -rf /tmp/m-a
	git add results && git commit -a -m "benchmark results" && git push

all: lispworks sbcl ccl allegro
