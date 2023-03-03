(load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))

#+lispworks
(progn
  (setf *redefinition-action* nil)
  (setf *handle-warn-on-redefinition* nil))

(ql:quickload '(:cl-date-time-parser
                :cl-fad
                :cl-json
                :fare-memoization
                :gzip-stream
                :jonathan
                :shasht
                :for
                :manardb
                :pcall
                :salza2
                :zs3
                :uiop
                :usocket))

;;(load "package.lisp")

(require 'asdf)
(asdf:operate 'asdf:load-op 'metis)

#+(or ccl clisp ecl)
(ql:quickload "trivial-dump-core")

#+sbcl
(sb-ext:save-lisp-and-die "dist/sbcl/metis" :compression 5 :executable t :toplevel 'metis:main :save-runtime-options t)

#+(or ccl ccl64 )
(trivial-dump-core:save-executable "../dist/ccl/metis" #'metis:main)

#+cmucl
;;(save-lisp :executable t
(trivial-dump-core:save-executable "../dist/cmucl/metis" #'metis:main)

#+clisp
(ext:saveinitmem "dist/clisp/metis" :init-function #'metis:main :executable t :quiet t)

#+lispworks
(deliver 'metis:main "../dist/lispworks/metis"
         0
         :keep-package-manipulation t
         :multiprocessing t
         :keep-eval t
         :keep-fasl-dump t
         :keep-editor t
         :keep-foreign-symbols t
         :keep-function-name t
         :keep-gc-cursor t
         :keep-keyword-names t
         :keep-lisp-reader t
         :keep-macros t
         :keep-modules t
         :keep-top-level t
         :license-info nil
         :keep-walker t
         :KEEP-PRETTY-PRINTER t)

#+allegro
(let ((lfiles '("package.lisp"
                "bench.lisp"
                "utils.lisp"
                "ctcl.lisp"
                "database.lisp"
                "db-manardb.lisp"
                "main.lisp"
                "flows.lisp")))
  (mapcar #'compile-file lfiles)
  (cl-user::generate-executable
   "../dist/allegro/metis"
   '("package.fasl"
     "bench.fasl"
     "utils.fasl"
     "database.fasl"
     "db-manardb.fasl"
     "ctcl.fasl"
     "main.fasl"
     "flows.fasl")
   :runtime :partners
   :show-window :shownoactivate
   :system-dlls-path "system-dlls/"
   :temporary-directory #P"/tmp/"
   :verbose nil
   :discard-compiler nil
   :discard-local-name-info t
   :discard-source-file-info t
   :discard-xref-info t
   :ignore-command-line-arguments t
   :include-compiler t
   :include-composer nil
   :include-debugger t
   :include-devel-env nil
   :include-ide nil
   :runtime :partners
   :suppress-allegro-cl-banner t
   :newspace 1677721600
   :oldspace 3355443200))
