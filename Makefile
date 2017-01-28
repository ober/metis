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

bench: all
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" metis-sbcl s ~/ct-test > results/sbcl
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" metis-lispworks s ~/ct-test > results/lispworks
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" metis-allegro s ~/ct-test > results/allegro
	rm -rf /tmp/m-a && mkdir /tmp/m-a
	METIS="/tmp/m-a/" metis-ccl s ~/ct-test > results/ccl
	rm -rf /tmp/m-a
	git add results && git commit -a -m "benchmark results" && git push

all: lispworks sbcl ccl allegro
