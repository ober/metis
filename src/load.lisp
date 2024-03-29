(ql:quickload '(
		:cl-fad
		:cl-json
		:cl-ssdb
		:fare-memoization
		:gzip-stream
		:manardb
		:pcall
		:usocket))

(princ "pkgdcl")
(load (compile-file "pkgdcl.lisp"))
(princ "database")
(load (compile-file "database.lisp"))
(load (compile-file "db-manardb.lisp"))
(load (compile-file "db-ssdb.lisp"))
(princ "bench")
(load (compile-file "bench.lisp"))
(princ "utils")
(load (compile-file "utils.lisp"))
(princ "ctcl")
(load (compile-file "ctcl.lisp"))
(princ "main")
(load (compile-file "main.lisp"))
(in-package :ctcl)
