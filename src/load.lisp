;;#-cmucl (load "~/quicklisp/setup.lisp")
(ql:quickload '(
		:cl-fad
		:cl-json
		:fare-memoization
		:gzip-stream
		:cl-base64
		:thnappy
		:manardb
		:pcall
		:usocket))

(princ "pkgdcl")
(load (compile-file "pkgdcl.lisp"))
(princ "database")
(load (compile-file "database.lisp"))
(load (compile-file "db-manardb.lisp"))
(princ "bench")
(load (compile-file "bench.lisp"))
(princ "utils")
(load (compile-file "utils.lisp"))
(princ "ctcl")
(load (compile-file "ctcl.lisp"))
(princ "main")
(load (compile-file "main.lisp"))
(in-package :ctcl)
