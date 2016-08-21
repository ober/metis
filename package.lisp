#+allegro (progn
	    (load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
	    (require :acache "acache-3.0.6.fasl")
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
  #+allegro (:use :db.allegrocache)
  (:export
   #:main
   #:run-bench))
