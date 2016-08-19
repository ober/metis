(load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
(ql:quickload '(:fare-memoization
		:cl-fad
		:gzip-stream
		:cl-json
		:s-sql
		:pcall
		:uiop
		:postmodern
		:cl-date-time-parser
		))

(load "package.lisp")

(require 'asdf)
(asdf:operate 'asdf:load-op 'metis)

#+(or ccl clisp ecl)
(ql:quickload "trivial-dump-core")

#+sbcl
(sb-ext:save-lisp-and-die "dist/sbcl/metis" :compression 5 :executable t :toplevel 'metis:main :save-runtime-options t)

#+(or ccl ccl64 )
(trivial-dump-core:save-executable "dist/ccl/metis" #'metis:main)

#+cmucl
;;(save-lisp :executable t
(trivial-dump-core:save-executable "dist/cmucl/metis" #'metis:main)

#+clisp
(ext:saveinitmem "dist/clisp/metis" :init-function #'metis:main :executable t :quiet t)

#+lispworks
(deliver 'metis:main "dist/lispworks/metis" 0 :keep-package-manipulation t :multiprocessing t :keep-eval t :keep-fasl-dump t :keep-editor t :keep-foreign-symbols t :keep-function-name t :keep-gc-cursor t :keep-keyword-names t :keep-lisp-reader t :keep-macros t :keep-modules t :keep-top-level t :license-info nil  :keep-walker t :KEEP-PRETTY-PRINTER t)

#+allegro
(progn
  (let ((lfiles '("package.lisp" "utils.lisp" "ctcl.lisp" "database.lisp" "main.lisp" "bench.lisp")))
    (mapcar #'compile-file lfiles)
    (cl-user::generate-executable "metis" '("package.fasl" "utils.fasl" "database.fasl" "ctcl.fasl" "main.fasl" "bench.fasl"))))
