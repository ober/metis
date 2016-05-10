(load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
(ql:quickload '(:fare-memoization :cl-fad :gzip-stream :cl-json :postmodern :s-sql :pcall :uiop))

(defpackage :ctcl
  (:use :cl :fare-memoization :cl-fad :gzip-stream :cl-json )
  #+allegro (:use :prof )
  )

