#+allegro (progn
	    (load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
	    (require :acache "acache-3.0.6.fasl")
	    (ql:quickload '(
			    :cl-date-time-parser
			    :cl-fad
			    :fare-memoization
			    :gzip-stream
			    :jonathan
			    :pcall
			    :postmodern
			    :manardb
			    :s-sql
			    :sqlite
			    :uiop
			    )))


(defpackage :metis
  (:use :cl )
  #+allegro (:use :db.allegrocache)
  (:export
   #:main
   #:run-bench))
