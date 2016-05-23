(load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
(ql:quickload '(:fare-memoization :cl-fad :gzip-stream :cl-json :s-sql :pcall :uiop :cl-store :postmodern))
(load "version.lisp")

(defpackage :ctcl
  (:use :cl :fare-memoization :cl-fad :gzip-stream :cl-json )
  #+allegro (:use :prof )
  )

