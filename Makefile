.DEFAULT_GOAL := usage
.PHONY: chicken

usage:
	@ echo "make all # create distributions for all installed lisps"


lispworks:
	@ mkdir -p dist/lispworks
	@ cd src && lispworks -build deliver.lisp

sbcl:
	@ mkdir -p dist/sbcl
	@ cat src/deliver.lisp|sbcl --control-stack-size 2048 --dynamic-space-size 18096  2>&1 > /dev/null #--control-stack-size 2048  #--disable-debugger

ccl:
	@ mkdir -p dist/ccl || true
	@ cd src && cat deliver.lisp|ccl64

allegro:
	@ rm -rf dist/allegro metis
	@ mkdir -p dist/allegro
	@ cd src && cat deliver.lisp|allegro
	@ mv src/metis/* dist/allegro
	@ rm -rf src/metis

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
