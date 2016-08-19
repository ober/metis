#+allegro (progn
	    (load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
	    (ql:quickload '(
			    :fare-memoization
			    :cl-fad
			    :gzip-stream
			    :cl-json
			    :s-sql
			    :pcall
			    :uiop
			    :cl-date-time-parser
			    :postmodern)))


(defpackage :metis
  (:use :cl )
  (:export
   #:main
   #:run-bench))
